# -*- coding: utf-8 -*-
"""
Renderiza un preview de UNA carta (front + back) en un PDF A4 horizontal.

Uso típico:
  python pipeline/cards/render_card_preview.py --deck pipeline/data/processed/deck_I.csv --card-id I-edd02c7d
"""
from __future__ import annotations

import argparse
import hashlib
from io import BytesIO
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import qrcode
from qrcode.constants import ERROR_CORRECT_H

from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


# -----------------------------
# Helpers
# -----------------------------
def mm_to_pt(x_mm: float) -> float:
    return x_mm * mm


def safe_str(x) -> str:
    return str(x if x is not None else "").strip()


def normalize_year(y) -> str:
    """
    Convierte year a entero si viene como 1978.0, '1978.0', etc.
    Si falta o no es parseable -> '—'
    """
    s = safe_str(y)
    if not s or s.lower() in {"nan", "none"}:
        return "—"
    try:
        v = float(s)
        if v != v:  # NaN
            return "—"
        return str(int(round(v)))
    except Exception:
        if s.isdigit():
            return s
        return "—"


def parse_owners(row: dict) -> List[str]:
    """
    Intenta sacar owners desde columnas típicas.
    Devuelve lista única y ordenada alfabéticamente (case-insensitive).
    """
    candidates = [
        row.get("owners_display"),
        row.get("owners"),
        row.get("playlist_owners"),
        row.get("playlist_owner"),
        row.get("owner"),
        row.get("owners_canon"),
    ]
    raw = ""
    for c in candidates:
        raw = safe_str(c)
        if raw:
            break
    if not raw:
        return []
    for sep in ["|", ";", "·", "•", "/", "\\"]:
        raw = raw.replace(sep, ",")
    parts = [p.strip() for p in raw.split(",") if p.strip()]

    seen = set()
    out = []
    for p in parts:
        k = p.casefold()
        if k not in seen:
            seen.add(k)
            out.append(p)
    out.sort(key=lambda s: s.casefold())
    return out


def stable_palette_pick(key: str, palette: List[str]) -> str:
    """
    Color determinista en función de key (para que una carta siempre tenga el mismo color).
    """
    if not palette:
        return "#FFFFFF"
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(palette)
    return palette[idx]


def try_register_fonts(font_dir: Path) -> dict:
    """
    Registra fuentes opcionales si existen en pipeline/cards/assets/fonts.

    Espera (cualquiera de estas combinaciones):
      - Inter-Regular.ttf / Inter-Bold.ttf / Inter-Italic.ttf / Inter-BoldItalic.ttf
      - Montserrat-Regular.ttf / Montserrat-Bold.ttf / Montserrat-Italic.ttf / Montserrat-BoldItalic.ttf

    Si no están, usa Helvetica/Helvetica-Bold/Helvetica-Oblique.
    """
    font_dir = Path(font_dir)
    mapping = {
        "regular": "Helvetica",
        "bold": "Helvetica-Bold",
        "italic": "Helvetica-Oblique",
        "bold_italic": "Helvetica-BoldOblique",
    }
    if not font_dir.exists():
        return mapping

    families = ["Inter", "Montserrat"]
    for fam in families:
        reg = font_dir / f"{fam}-Regular.ttf"
        bold = font_dir / f"{fam}-Bold.ttf"
        ita = font_dir / f"{fam}-Italic.ttf"
        bi = font_dir / f"{fam}-BoldItalic.ttf"

        if reg.exists() and bold.exists():
            pdfmetrics.registerFont(TTFont(f"{fam}-R", str(reg)))
            pdfmetrics.registerFont(TTFont(f"{fam}-B", str(bold)))
            mapping["regular"] = f"{fam}-R"
            mapping["bold"] = f"{fam}-B"

            if ita.exists():
                pdfmetrics.registerFont(TTFont(f"{fam}-I", str(ita)))
                mapping["italic"] = f"{fam}-I"
            if bi.exists():
                pdfmetrics.registerFont(TTFont(f"{fam}-BI", str(bi)))
                mapping["bold_italic"] = f"{fam}-BI"
            else:
                mapping["bold_italic"] = (
                    mapping["italic"] if ita.exists() else mapping["bold"]
                )
            return mapping

    return mapping


def wrap_two_lines(
    c, text: str, font_name: str, font_size: int, max_width: float
) -> Tuple[str, str]:
    """
    Intenta partir en 2 líneas por espacios para maximizar ocupación sin pasarse del ancho.
    Devuelve (line1, line2). line2 puede ser "" si cabe en una.
    """
    text = safe_str(text)
    if not text:
        return "", ""

    if c.stringWidth(text, font_name, font_size) <= max_width:
        return text, ""

    words = text.split()
    if len(words) == 1:
        return text, ""

    best = (text, "")
    best_balance = float("inf")

    for i in range(1, len(words)):
        l1 = " ".join(words[:i])
        l2 = " ".join(words[i:])
        w1 = c.stringWidth(l1, font_name, font_size)
        w2 = c.stringWidth(l2, font_name, font_size)
        if w1 <= max_width and w2 <= max_width:
            balance = abs(w1 - w2)
            if balance < best_balance:
                best_balance = balance
                best = (l1, l2)

    if best == (text, ""):
        l1 = ""
        l2 = ""
        for w in words:
            candidate = (l1 + " " + w).strip()
            if c.stringWidth(candidate, font_name, font_size) <= max_width or not l1:
                l1 = candidate
            else:
                l2 = (l2 + " " + w).strip()
        best = (l1, l2)

    return best


def fit_title_two_lines(
    c,
    text: str,
    x_center: float,
    y_top: float,
    max_width: float,
    font_name: str,
    start_size: int = 20,
    min_size: int = 10,
    line_gap: float = 2.0,
) -> Tuple[int, str, str]:
    """
    Ajusta tamaño para que el título pueda ocupar 1-2 líneas antes de reducirse.
    Dibuja centrado; devuelve (final_size, line1, line2).
    """
    text = safe_str(text)
    if not text:
        return min_size, "", ""

    size = start_size
    while size >= min_size:
        c.setFont(font_name, size)
        l1, l2 = wrap_two_lines(c, text, font_name, size, max_width)
        if l1 and c.stringWidth(l1, font_name, size) <= max_width:
            ok2 = (not l2) or (c.stringWidth(l2, font_name, size) <= max_width)
            if ok2:
                c.drawCentredString(x_center, y_top, l1)
                if l2:
                    c.drawCentredString(x_center, y_top - (size + line_gap), l2)
                return size, l1, l2
        size -= 1

    c.setFont(font_name, min_size)
    trimmed = text
    while trimmed and c.stringWidth(trimmed + "…", font_name, min_size) > max_width:
        trimmed = trimmed[:-1]
    c.drawCentredString(x_center, y_top, (trimmed + "…") if trimmed else "…")
    return min_size, trimmed + "…", ""


def make_qr_imagereader(data: str, border: int = 2) -> ImageReader:
    """
    Genera QR y lo devuelve como ImageReader (PNG en memoria).
    """
    qr = qrcode.QRCode(
        version=None,
        error_correction=ERROR_CORRECT_H,
        box_size=10,
        border=border,
    )
    qr.add_data(data)
    qr.make(fit=True)

    qr_img = qr.make_image(fill_color="black", back_color="white")
    get_img = getattr(qr_img, "get_image", None)
    if callable(get_img):
        qr_img = get_img()
    qr_img = qr_img.convert("RGB")

    buf = BytesIO()
    qr_img.save(buf, format="PNG")
    buf.seek(0)
    return ImageReader(buf)


# -----------------------------
# Drawing
# -----------------------------
NEON_BG_PALETTE = [
    "#00D1FF",  # cyan
    "#A855F7",  # purple
    "#FF7A00",  # orange
    "#FFD400",  # yellow
    "#FF3BD4",  # pink
    "#7CFF00",  # neon green
    "#00F5D4",  # aqua
]


def draw_full_bleed_image(
    c, img_reader: ImageReader, x: float, y: float, w: float, h: float
) -> None:
    c.drawImage(img_reader, x, y, w, h, mask="auto")


def draw_card_front(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    spotify_url: str,
    bg_path: Path,
    show_debug_title: bool,
    title_debug: str,
    fonts: dict,
) -> None:
    if bg_path.exists():
        bg_reader = ImageReader(str(bg_path))
        draw_full_bleed_image(c, bg_reader, x, y, w, h)
    else:
        c.setFillColor(colors.black)
        c.rect(x, y, w, h, fill=1, stroke=0)

    qr_reader = make_qr_imagereader(spotify_url, border=2)

    qr_size = min(w, h) * 0.62
    qr_x = x + (w - qr_size) / 2
    qr_y = y + (h - qr_size) / 2

    pad = qr_size * 0.06
    c.setFillColor(colors.white)
    c.rect(
        qr_x - pad, qr_y - pad, qr_size + 2 * pad, qr_size + 2 * pad, fill=1, stroke=0
    )
    c.drawImage(qr_reader, qr_x, qr_y, qr_size, qr_size, mask="auto")

    if show_debug_title and title_debug:
        band_h = h * 0.18
        band_y = y + (h - band_h) / 2
        c.setFillColor(colors.HexColor("#FFD400"))
        c.rect(x, band_y, w, band_h, fill=1, stroke=0)

        c.setFillColor(colors.HexColor("#FF2D2D"))
        max_w = w * 0.86
        fit_title_two_lines(
            c,
            title_debug,
            x + w / 2,
            band_y + band_h * 0.62,
            max_w,
            font_name=fonts["bold"],
            start_size=16,
            min_size=10,
            line_gap=1.0,
        )


def draw_card_back(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    artists: str,
    year: str,
    owners: List[str],
    expansion_code: str,
    fonts: dict,
) -> None:
    bg_hex = stable_palette_pick(
        f"{title}|{artists}|{year}|{expansion_code}", NEON_BG_PALETTE
    )
    c.setFillColor(colors.HexColor(bg_hex))
    c.rect(x, y, w, h, fill=1, stroke=0)

    c.setStrokeColor(colors.black)
    c.setLineWidth(0.6)
    c.rect(x, y, w, h, fill=0, stroke=1)

    c.setFillColor(colors.black)

    pad_x = w * 0.08
    max_w = w - 2 * pad_x

    header = f"EXP {safe_str(expansion_code) or 'I'}"
    c.setFont(fonts["bold"], 10)
    c.drawString(x + pad_x, y + h - (h * 0.10), header)

    fit_title_two_lines(
        c,
        title,
        x + w / 2,
        y + h * 0.84,
        max_w,
        font_name=fonts["bold"],
        start_size=20,
        min_size=10,
        line_gap=2.0,
    )

    year_str = normalize_year(year)
    year_size = 56 if year_str != "—" else 44
    c.setFont(fonts["bold"], year_size)
    c.drawCentredString(x + w / 2, y + h * 0.50, year_str)

    artists_y = y + h * 0.36
    c.setFont(fonts["regular"], 14)
    artists_text = safe_str(artists) or "—"
    while (
        c.stringWidth(artists_text, fonts["regular"], 14) > max_w
        and len(artists_text) > 3
    ):
        artists_text = artists_text[:-1]
    c.drawCentredString(x + w / 2, artists_y, artists_text)

    footer_y = y + h * 0.12
    owners_str = ", ".join(owners) if owners else ""
    if owners_str:
        footer = f"({owners_str}) - Exp {safe_str(expansion_code) or 'I'}"
    else:
        footer = f"Exp {safe_str(expansion_code) or 'I'}"

    c.setFont(fonts["italic"], 10)
    footer_txt = footer
    if c.stringWidth(footer_txt, fonts["italic"], 10) > max_w:
        while (
            footer_txt and c.stringWidth(footer_txt + "…", fonts["italic"], 10) > max_w
        ):
            footer_txt = footer_txt[:-1]
        footer_txt = (footer_txt + "…") if footer_txt else "…"
    c.drawCentredString(x + w / 2, footer_y, footer_txt)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--deck",
        default="pipeline/data/processed/deck_I.csv",
        help="CSV del deck (build_deck.py)",
    )
    ap.add_argument(
        "--card-id", default="", help="card_id concreto (si vacío usa la primera fila)"
    )
    ap.add_argument(
        "--size-mm",
        type=float,
        default=65.0,
        help="Tamaño de la carta en mm (cuadrada)",
    )
    ap.add_argument(
        "--out",
        default="",
        help="Salida PDF (si vacío -> pipeline/reports/card_preview_<card_id>.pdf)",
    )
    ap.add_argument(
        "--bg",
        default="pipeline/cards/assets/back_bg.png",
        help="Imagen de fondo para el FRONT",
    )
    ap.add_argument(
        "--debug-front-title",
        action="store_true",
        help="Pone una banda con el título en el FRONT (debug)",
    )
    args = ap.parse_args()

    deck_path = Path(args.deck)
    if not deck_path.exists():
        raise FileNotFoundError(f"No existe: {deck_path}")

    df = pd.read_csv(deck_path).fillna("")
    if df.empty:
        raise SystemExit("Deck vacío.")

    if args.card_id:
        row = df[df.get("card_id", "") == args.card_id]
        if row.empty:
            row = df[df["card_id"].astype(str).str.strip() == args.card_id.strip()]
        if row.empty:
            raise SystemExit(f"No encuentro card_id={args.card_id} en {deck_path}")
        r = row.iloc[0].to_dict()
    else:
        r = df.iloc[0].to_dict()

    title = safe_str(
        r.get("title_display") or r.get("title_canon") or r.get("title") or ""
    )
    artists = safe_str(
        r.get("artists_display") or r.get("artists_canon") or r.get("artists") or ""
    )
    year = r.get("year", "")
    spotify_url = safe_str(r.get("spotify_url") or r.get("spotify_link") or "")
    expansion = safe_str(r.get("expansion_code") or r.get("expansion") or "I")
    owners = parse_owners(r)

    if not spotify_url:
        raise SystemExit("Esta carta no tiene spotify_url (missing_spotify_url=1).")

    fonts = try_register_fonts(Path("pipeline/cards/assets/fonts"))

    page_w, page_h = mm_to_pt(297), mm_to_pt(210)  # A4 horizontal
    card = mm_to_pt(args.size_mm)
    gap = mm_to_pt(12)

    total_w = card * 2 + gap
    x0 = (page_w - total_w) / 2
    y0 = (page_h - card) / 2

    out_dir = Path("pipeline/reports")
    out_dir.mkdir(parents=True, exist_ok=True)

    card_id = safe_str(r.get("card_id") or "")
    if not args.out:
        out_pdf = out_dir / f"card_preview_{card_id or 'first'}.pdf"
    else:
        out_pdf = Path(args.out)
        out_pdf.parent.mkdir(parents=True, exist_ok=True)

    c = canvas.Canvas(str(out_pdf), pagesize=(page_w, page_h))

    draw_card_front(
        c,
        x0,
        y0,
        card,
        card,
        spotify_url=spotify_url,
        bg_path=Path(args.bg),
        show_debug_title=bool(args.debug_front_title),
        title_debug=title,
        fonts=fonts,
    )
    draw_card_back(
        c,
        x0 + card + gap,
        y0,
        card,
        card,
        title=title,
        artists=artists,
        year=year,
        owners=owners,
        expansion_code=expansion,
        fonts=fonts,
    )

    c.showPage()
    c.save()
    print(f"OK -> {out_pdf}")


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""
Genera PDFs de impresión (A4) con FRONTS y BACKS en páginas alternas.

- Sin margen ENTRE cartas (gap=0): solo líneas finas para guiar el corte.
- Líneas blancas en el FRONT (para que se vean sobre negro), negras en el BACK.
- Genera por defecto:
    * 4x3 flip-short
    * 4x3 flip-long
    * 3x3 flip-short
    * 3x3 flip-long
  y además versiones "match" donde el BACK se coloca en el MISMO orden que el FRONT
  (sin espejo/flip) para pruebas en imprenta.

Ejemplos:
  python pipeline/cards/render_print_sheets.py --deck pipeline/data/processed/deck_I.csv

  # Test rápido 1 hoja (2 páginas):
  python pipeline/cards/render_print_sheets.py --deck pipeline/data/processed/deck_I.csv --max-sheets 1
"""
from __future__ import annotations

import argparse
import hashlib
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
    s = safe_str(y)
    if not s or s.lower() in {"nan", "none"}:
        return "—"
    try:
        v = float(s)
        if v != v:
            return "—"
        return str(int(round(v)))
    except Exception:
        if s.isdigit():
            return s
        return "—"


def parse_owners(row: dict) -> List[str]:
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
    if not palette:
        return "#FFFFFF"
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    idx = int(h[:8], 16) % len(palette)
    return palette[idx]


def try_register_fonts(font_dir: Path) -> dict:
    mapping = {
        "regular": "Helvetica",
        "bold": "Helvetica-Bold",
        "italic": "Helvetica-Oblique",
        "bold_italic": "Helvetica-BoldOblique",
    }
    font_dir = Path(font_dir)
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
) -> None:
    text = safe_str(text)
    if not text:
        return
    size = start_size
    while size >= min_size:
        c.setFont(font_name, size)
        l1, l2 = wrap_two_lines(c, text, font_name, size, max_width)
        ok1 = (not l1) or (c.stringWidth(l1, font_name, size) <= max_width)
        ok2 = (not l2) or (c.stringWidth(l2, font_name, size) <= max_width)
        if ok1 and ok2 and l1:
            c.drawCentredString(x_center, y_top, l1)
            if l2:
                c.drawCentredString(x_center, y_top - (size + line_gap), l2)
            return
        size -= 1

    c.setFont(font_name, min_size)
    trimmed = text
    while trimmed and c.stringWidth(trimmed + "…", font_name, min_size) > max_width:
        trimmed = trimmed[:-1]
    c.drawCentredString(x_center, y_top, (trimmed + "…") if trimmed else "…")


def make_qr_imagereader_cached(
    cache: Dict[str, ImageReader], data: str, border: int = 2
) -> ImageReader:
    if data in cache:
        return cache[data]
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
    img = ImageReader(buf)
    cache[data] = img
    return img


# -----------------------------
# Drawing
# -----------------------------
NEON_BG_PALETTE = [
    "#00D1FF",
    "#A855F7",
    "#FF7A00",
    "#FFD400",
    "#FF3BD4",
    "#7CFF00",
    "#00F5D4",
]


def draw_full_bleed_image(
    c, img_reader: ImageReader, x: float, y: float, w: float, h: float
) -> None:
    c.drawImage(img_reader, x, y, w, h, mask="auto")


def draw_cut_box(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    stroke_color,
    line_w: float,
) -> None:
    c.setStrokeColor(stroke_color)
    c.setLineWidth(line_w)
    c.rect(x, y, w, h, fill=0, stroke=1)


def draw_front_card(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    spotify_url: str,
    bg_reader: Optional[ImageReader],
    qr_cache: Dict[str, ImageReader],
    show_debug_title: bool,
    title_debug: str,
    fonts: dict,
) -> None:
    if bg_reader is not None:
        draw_full_bleed_image(c, bg_reader, x, y, w, h)
    else:
        c.setFillColor(colors.black)
        c.rect(x, y, w, h, fill=1, stroke=0)

    qr_reader = make_qr_imagereader_cached(qr_cache, spotify_url, border=2)
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


def draw_back_card(
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

    draw_cut_box(c, x, y, w, h, colors.black, 0.6)

    c.setFillColor(colors.black)
    pad_x = w * 0.08
    max_w = w - 2 * pad_x

    c.setFont(fonts["bold"], 10)
    c.drawString(
        x + pad_x, y + h - (h * 0.10), f"EXP {safe_str(expansion_code) or 'I'}"
    )

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

    c.setFont(fonts["regular"], 14)
    art = safe_str(artists) or "—"
    while c.stringWidth(art, fonts["regular"], 14) > max_w and len(art) > 3:
        art = art[:-1]
    c.drawCentredString(x + w / 2, y + h * 0.36, art)

    owners_str = ", ".join(owners) if owners else ""
    footer = (
        f"({owners_str}) - Exp {safe_str(expansion_code) or 'I'}"
        if owners_str
        else f"Exp {safe_str(expansion_code) or 'I'}"
    )
    c.setFont(fonts["italic"], 10)
    footer_txt = footer
    if c.stringWidth(footer_txt, fonts["italic"], 10) > max_w:
        while (
            footer_txt and c.stringWidth(footer_txt + "…", fonts["italic"], 10) > max_w
        ):
            footer_txt = footer_txt[:-1]
        footer_txt = (footer_txt + "…") if footer_txt else "…"
    c.drawCentredString(x + w / 2, y + h * 0.12, footer_txt)


# -----------------------------
# Layout / Ordering
# -----------------------------
def chunk(lst: List[dict], n: int) -> List[List[dict]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def order_back_indices(rows: int, cols: int, mode: str) -> List[int]:
    """
    Índices (0..rows*cols-1) para pintar el BACK según modo:
      - match: igual que FRONT
      - flip-short: espejo vertical (invierte filas)
      - flip-long: espejo horizontal (invierte columnas)
    """
    n = rows * cols
    front = list(range(n))

    def rc(i):
        return divmod(i, cols)

    def idx(r, c):
        return r * cols + c

    if mode == "match":
        return front

    if mode == "flip-short":
        out = []
        for i in front:
            r, c = rc(i)
            out.append(idx((rows - 1) - r, c))
        return out

    if mode == "flip-long":
        out = []
        for i in front:
            r, c = rc(i)
            out.append(idx(r, (cols - 1) - c))
        return out

    raise ValueError(f"Modo desconocido: {mode}")


def compute_grid(
    page_w: float, page_h: float, card_w: float, card_h: float, rows: int, cols: int
) -> Tuple[float, float]:
    grid_w = cols * card_w
    grid_h = rows * card_h
    x0 = (page_w - grid_w) / 2
    y0 = (page_h - grid_h) / 2
    return x0, y0


def generate_pdf(
    out_pdf: Path,
    cards: List[dict],
    rows: int,
    cols: int,
    card_mm: float,
    bg_path: Path,
    mode: str,
    max_sheets: Optional[int],
    show_debug_front_title: bool,
) -> None:
    out_pdf.parent.mkdir(parents=True, exist_ok=True)

    page_w, page_h = mm_to_pt(210), mm_to_pt(297)  # A4 portrait
    card = mm_to_pt(card_mm)

    x0, y0 = compute_grid(page_w, page_h, card, card, rows, cols)

    fonts = try_register_fonts(Path("pipeline/cards/assets/fonts"))

    bg_reader = ImageReader(str(bg_path)) if bg_path.exists() else None
    qr_cache: Dict[str, ImageReader] = {}

    per_sheet = rows * cols
    sheets = chunk(cards, per_sheet)
    if max_sheets is not None:
        sheets = sheets[:max_sheets]

    total_sheets = len(sheets)
    total_pages = total_sheets * 2

    print(
        f"[{out_pdf.name}] Generando {total_sheets} sheets -> {total_pages} páginas (modo={mode}, grid={rows}x{cols})",
        flush=True,
    )

    c = canvas.Canvas(str(out_pdf), pagesize=(page_w, page_h))

    front_cut_color = colors.white
    back_cut_color = colors.black
    cut_lw = 0.4

    back_idx_map = order_back_indices(rows, cols, mode)

    for si, sheet_cards in enumerate(sheets, start=1):
        if len(sheet_cards) < per_sheet:
            sheet_cards = sheet_cards + ([{}] * (per_sheet - len(sheet_cards)))

        # FRONT
        for r in range(rows):
            for col in range(cols):
                i = r * cols + col
                card_data = sheet_cards[i]

                cx = x0 + col * card
                cy = y0 + (rows - 1 - r) * card

                spotify_url = safe_str(card_data.get("spotify_url") or "")
                title = safe_str(
                    card_data.get("title_display") or card_data.get("title_canon") or ""
                )
                if spotify_url:
                    draw_front_card(
                        c,
                        cx,
                        cy,
                        card,
                        card,
                        spotify_url=spotify_url,
                        bg_reader=bg_reader,
                        qr_cache=qr_cache,
                        show_debug_title=show_debug_front_title,
                        title_debug=title,
                        fonts=fonts,
                    )
                else:
                    c.setFillColor(colors.black)
                    c.rect(cx, cy, card, card, fill=1, stroke=0)

                draw_cut_box(c, cx, cy, card, card, front_cut_color, cut_lw)

        c.showPage()

        # BACK
        for r in range(rows):
            for col in range(cols):
                i_front = r * cols + col
                i_back = back_idx_map[i_front]
                card_data = sheet_cards[i_back]

                cx = x0 + col * card
                cy = y0 + (rows - 1 - r) * card

                title = safe_str(
                    card_data.get("title_display") or card_data.get("title_canon") or ""
                )
                artists = safe_str(
                    card_data.get("artists_display")
                    or card_data.get("artists_canon")
                    or ""
                )
                year = card_data.get("year", "")
                expansion = safe_str(card_data.get("expansion_code") or "I")
                owners = parse_owners(card_data)

                if title or artists or safe_str(year):
                    draw_back_card(
                        c,
                        cx,
                        cy,
                        card,
                        card,
                        title=title,
                        artists=artists,
                        year=year,
                        owners=owners,
                        expansion_code=expansion,
                        fonts=fonts,
                    )
                else:
                    c.setFillColor(colors.white)
                    c.rect(cx, cy, card, card, fill=1, stroke=0)

                draw_cut_box(c, cx, cy, card, card, back_cut_color, cut_lw)

        c.showPage()

        if si == 1 or si % 10 == 0 or si == total_sheets:
            pct = (si / total_sheets) * 100 if total_sheets else 100
            print(
                f"[{out_pdf.name}] sheet {si}/{total_sheets} ({pct:.1f}%)", flush=True
            )

    c.save()
    print(f"[{out_pdf.name}] OK -> {out_pdf}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--deck", default="pipeline/data/processed/deck_I.csv")
    ap.add_argument("--out-dir", default="pipeline/reports")
    ap.add_argument("--card-mm", type=float, default=65.0)
    ap.add_argument("--bg", default="pipeline/cards/assets/back_bg.png")
    ap.add_argument(
        "--max-sheets",
        type=int,
        default=None,
        help="Para test: limita nº de sheets (cada sheet = 2 páginas)",
    )
    ap.add_argument(
        "--debug-front-title",
        action="store_true",
        help="Banda con título en el FRONT (debug)",
    )
    ap.add_argument(
        "--only",
        default="",
        help="(opcional) genera solo: 4x3_short, 4x3_long, 3x3_short, 3x3_long, 4x3_match, 3x3_match",
    )
    args = ap.parse_args()

    deck_path = Path(args.deck)
    if not deck_path.exists():
        raise FileNotFoundError(f"No existe: {deck_path}")

    df = pd.read_csv(deck_path).fillna("")
    if df.empty:
        raise SystemExit("Deck vacío.")
    cards = df.to_dict(orient="records")

    out_dir = Path(args.out_dir)
    bg_path = Path(args.bg)

    variants = [
        ("4x3_short", 4, 3, "flip-short"),
        ("4x3_long", 4, 3, "flip-long"),
        ("3x3_short", 3, 3, "flip-short"),
        ("3x3_long", 3, 3, "flip-long"),
        ("4x3_match", 4, 3, "match"),
        ("3x3_match", 3, 3, "match"),
    ]

    if args.only:
        wanted = set([s.strip().lower() for s in args.only.split(",") if s.strip()])
        variants = [v for v in variants if v[0].lower() in wanted]
        if not variants:
            raise SystemExit(
                "Opciones --only: 4x3_short,4x3_long,3x3_short,3x3_long,4x3_match,3x3_match"
            )

    for name, rows, cols, mode in variants:
        out_pdf = out_dir / f"print_{name}.pdf"
        generate_pdf(
            out_pdf=out_pdf,
            cards=cards,
            rows=rows,
            cols=cols,
            card_mm=args.card_mm,
            bg_path=bg_path,
            mode=mode,
            max_sheets=args.max_sheets,
            show_debug_front_title=bool(args.debug_front_title),
        )


if __name__ == "__main__":
    main()

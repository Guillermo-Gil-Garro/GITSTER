# -*- coding: utf-8 -*-
"""
GITSTER — Render de PDFs de impresión (A4) con FRONTS y BACKS en páginas alternas.

(v5) BACK con fondos de imagen (1 por hoja):
- NO genera colores/degradados por código.
- Usa imágenes (PNG/JPG/WEBP) como background del BACK, estiradas/encajadas al área del grid.

FRONT:
- Fondo a sangre con pipeline/cards/assets/back_bg.png
- QR centrado, tamaño configurable (por defecto 25mm)

BACK:
- Fondo: 1 imagen por hoja (por página BACK), aplicada solo al área del grid.
- Texto negro (título 2 líneas máx, año grande, artistas, footer cursiva).

VARIANTES:
- print_4x3_short.pdf / print_4x3_long.pdf / print_3x3_short.pdf / print_3x3_long.pdf
- print_4x3_match.pdf / print_3x3_match.pdf

Notas impresión:
- *short* = “flip short edge”
- *long*  = “flip long edge”
- *match* = back con misma disposición que front (para validar a contraluz)
"""
from __future__ import annotations

import argparse
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
        return s if s.isdigit() else "—"


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
    seen, out = set(), []
    for p in parts:
        k = p.casefold()
        if k not in seen:
            seen.add(k)
            out.append(p)
    out.sort(key=lambda s: s.casefold())
    return out


def try_register_fonts(font_dir: Path) -> dict:
    """
    Si hay TTF en pipeline/cards/assets/fonts/ se registran.
    Fallback: Helvetica.
    """
    mapping = {
        "regular": "Helvetica",
        "bold": "Helvetica-Bold",
        "italic": "Helvetica-Oblique",
        "bold_italic": "Helvetica-BoldOblique",
    }
    font_dir = Path(font_dir)
    if not font_dir.exists():
        return mapping

    for fam in ["Inter", "Montserrat"]:
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
                mapping["bold_italic"] = mapping["italic"] if ita.exists() else mapping["bold"]
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
        l1, l2 = "", ""
        for w in words:
            cand = (l1 + " " + w).strip()
            if c.stringWidth(cand, font_name, font_size) <= max_width or not l1:
                l1 = cand
            else:
                l2 = (l2 + " " + w).strip()
        best = (l1, l2)

    return best


def fit_two_lines_centered(
    c: canvas.Canvas,
    text: str,
    x_center: float,
    y_top: float,
    max_width: float,
    font_name: str,
    start_size: int,
    min_size: int,
    line_gap: float,
) -> Tuple[int, str, str]:
    text = safe_str(text)
    if not text:
        return min_size, "", ""

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
            return size, l1, l2
        size -= 1

    c.setFont(font_name, min_size)
    trimmed = text
    while trimmed and c.stringWidth(trimmed + "…", font_name, min_size) > max_width:
        trimmed = trimmed[:-1]
    out = (trimmed + "…") if trimmed else "…"
    c.drawCentredString(x_center, y_top, out)
    return min_size, out, ""


def fit_single_line_centered(
    c: canvas.Canvas,
    text: str,
    x_center: float,
    y: float,
    max_width: float,
    font_name: str,
    start_size: int,
    min_size: int,
) -> int:
    text = safe_str(text)
    if not text:
        return min_size

    size = start_size
    while size >= min_size:
        if c.stringWidth(text, font_name, size) <= max_width:
            c.setFont(font_name, size)
            c.drawCentredString(x_center, y, text)
            return size
        size -= 1

    c.setFont(font_name, min_size)
    trimmed = text
    while trimmed and c.stringWidth(trimmed + "…", font_name, min_size) > max_width:
        trimmed = trimmed[:-1]
    out = (trimmed + "…") if trimmed else "…"
    c.drawCentredString(x_center, y, out)
    return min_size


# -----------------------------
# QR
# -----------------------------
QR_BORDER_MODULES = 2  # reduce borde blanco sin arriesgar demasiado


def make_qr_imagereader_cached(
    cache: Dict[str, ImageReader], data: str, border: int = QR_BORDER_MODULES
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
# Background images
# -----------------------------
def list_bg_files(bg_dir: Path) -> List[Path]:
    bg_dir = Path(bg_dir)
    if not bg_dir.exists():
        return []
    exts = {".png", ".jpg", ".jpeg", ".webp"}
    files = [p for p in bg_dir.iterdir() if p.is_file() and p.suffix.lower() in exts]
    files.sort(key=lambda p: p.name.lower())
    return files


def draw_image_cover_clipped(
    c: canvas.Canvas,
    img_reader: ImageReader,
    x: float,
    y: float,
    w: float,
    h: float,
) -> None:
    """
    Dibujo “stretch” con clip: encaja la imagen EXACTA al grid (deforma si hace falta),
    y se recorta al rectángulo para que nunca se salga.
    """
    c.saveState()
    p = c.beginPath()
    p.rect(x, y, w, h)
    c.clipPath(p, stroke=0, fill=0)
    c.drawImage(img_reader, x, y, w, h, mask="auto")
    c.restoreState()



def pick_bg_for_sheet(bg_files: List[Path], sheet_index_1based: int) -> Path:
    """1 imagen por hoja (por BACK page). Ciclado simple y predecible."""
    if not bg_files:
        raise FileNotFoundError("No hay fondos en --back-bg-dir.")
    return bg_files[(sheet_index_1based - 1) % len(bg_files)]


# -----------------------------
# Drawing (grid + cards)
# -----------------------------
def draw_cut_grid(
    c: canvas.Canvas,
    x0: float,
    y0: float,
    card: float,
    rows: int,
    cols: int,
    stroke_color,
    line_w: float,
) -> None:
    c.saveState()
    c.setStrokeColor(stroke_color)
    c.setLineWidth(line_w)
    w = cols * card
    h = rows * card
    c.rect(x0, y0, w, h, fill=0, stroke=1)
    for k in range(1, cols):
        x = x0 + k * card
        c.line(x, y0, x, y0 + h)
    for k in range(1, rows):
        y = y0 + k * card
        c.line(x0, y, x0 + w, y)
    c.restoreState()


def draw_front_card(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    spotify_url: str,
    bg_reader: Optional[ImageReader],
    qr_cache: Dict[str, ImageReader],
    qr_mm: float,
) -> None:
    if bg_reader is not None:
        c.drawImage(bg_reader, x, y, w, h, mask="auto")
    else:
        c.setFillColor(colors.black)
        c.rect(x, y, w, h, fill=1, stroke=0)

    qr_size = mm_to_pt(qr_mm)
    qr_size = min(qr_size, min(w, h) * 0.85)
    qr_x = x + (w - qr_size) / 2
    qr_y = y + (h - qr_size) / 2

    qr_reader = make_qr_imagereader_cached(qr_cache, spotify_url, border=QR_BORDER_MODULES)
    c.drawImage(qr_reader, qr_x, qr_y, qr_size, qr_size, mask="auto")


def draw_back_text_only(
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
    c.setFillColor(colors.black)
    pad_x = w * 0.08
    max_w = w - 2 * pad_x
    x_center = x + w / 2

    # Título (comedido)
    title_text = safe_str(title) or "—"
    fit_two_lines_centered(
        c,
        title_text,
        x_center=x_center,
        y_top=y + h * 0.80,
        max_width=max_w,
        font_name=fonts["bold"],
        start_size=16,
        min_size=9,
        line_gap=1.8,
    )

    # Año (grande)
    year_str = normalize_year(year)
    year_size = 64 if year_str != "—" else 52
    c.setFont(fonts["bold"], year_size)
    y_center_target = y + h * 0.52
    y_baseline = y_center_target - (0.35 * year_size)
    c.drawCentredString(x_center, y_baseline, year_str)

    # Artistas
    artists_text = safe_str(artists) or "—"
    fit_two_lines_centered(
        c,
        artists_text,
        x_center=x_center,
        y_top=y + h * 0.28,
        max_width=max_w,
        font_name=fonts["regular"],
        start_size=13,
        min_size=8,
        line_gap=1.4,
    )

    # Footer
    owners_str = ", ".join(owners) if owners else ""
    exp = safe_str(expansion_code) or "I"
    footer = f"({owners_str}) - Exp {exp}" if owners_str else f"Exp {exp}"
    fit_single_line_centered(
        c,
        footer,
        x_center=x_center,
        y=y + h * 0.10,
        max_width=max_w,
        font_name=fonts["italic"],
        start_size=10,
        min_size=7,
    )


# -----------------------------
# Layout / Ordering
# -----------------------------
def chunk(lst: List[dict], n: int) -> List[List[dict]]:
    return [lst[i: i + n] for i in range(0, len(lst), n)]


def order_back_indices(rows: int, cols: int, mode: str) -> List[int]:
    """
    - match     : igual que FRONT
    - flip-short: espejo vertical (invierte filas)
    - flip-long : espejo horizontal (invierte columnas)
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
        return [idx((rows - 1) - rc(i)[0], rc(i)[1]) for i in front]
    if mode == "flip-long":
        return [idx(rc(i)[0], (cols - 1) - rc(i)[1]) for i in front]
    raise ValueError(f"Modo desconocido: {mode}")


def compute_grid(page_w: float, page_h: float, card: float, rows: int, cols: int) -> Tuple[float, float]:
    grid_w = cols * card
    grid_h = rows * card
    x0 = (page_w - grid_w) / 2
    y0 = (page_h - grid_h) / 2
    return x0, y0


def generate_pdf(
    out_pdf: Path,
    cards: List[dict],
    rows: int,
    cols: int,
    card_mm: float,
    qr_mm: float,
    front_bg_path: Path,
    back_bg_dir: Path,
    mode: str,
    max_sheets: Optional[int],
) -> None:
    out_pdf.parent.mkdir(parents=True, exist_ok=True)

    page_w, page_h = mm_to_pt(210), mm_to_pt(297)  # A4 portrait
    card = mm_to_pt(card_mm)

    x0, y0 = compute_grid(page_w, page_h, card, rows, cols)
    grid_w = cols * card
    grid_h = rows * card

    fonts = try_register_fonts(Path("pipeline/cards/assets/fonts"))
    front_bg_reader = ImageReader(str(front_bg_path)) if front_bg_path.exists() else None
    qr_cache: Dict[str, ImageReader] = {}

    bg_files = list_bg_files(back_bg_dir)
    if not bg_files:
        raise FileNotFoundError(
            f"No encuentro fondos en {back_bg_dir}. "
            f"Crea la carpeta y mete PNG/JPG dentro (p.ej. g01.png...)."
        )

    per_sheet = rows * cols
    sheets = chunk(cards, per_sheet)
    if max_sheets is not None:
        sheets = sheets[:max_sheets]

    total_sheets = len(sheets)
    total_pages = total_sheets * 2
    print(
        f"[{out_pdf.name}] {total_sheets} sheets -> {total_pages} páginas "
        f"(modo={mode}, grid={rows}x{cols}, qr={qr_mm:.1f}mm, back_bg_dir={back_bg_dir})",
        flush=True,
    )

    c = canvas.Canvas(str(out_pdf), pagesize=(page_w, page_h))

    front_cut_color = colors.white
    back_cut_color = colors.black
    cut_lw = 0.6

    back_idx_map = order_back_indices(rows, cols, mode)
    back_bg_cache: Dict[str, ImageReader] = {}

    for si, sheet_cards in enumerate(sheets, start=1):
        if len(sheet_cards) < per_sheet:
            sheet_cards = sheet_cards + ([{}] * (per_sheet - len(sheet_cards)))

        # ---------- FRONT ----------
        for r in range(rows):
            for col in range(cols):
                i = r * cols + col
                card_data = sheet_cards[i]
                cx = x0 + col * card
                cy = y0 + (rows - 1 - r) * card

                spotify_url = safe_str(card_data.get("spotify_url") or "")
                if spotify_url:
                    draw_front_card(
                        c, cx, cy, card, card,
                        spotify_url=spotify_url,
                        bg_reader=front_bg_reader,
                        qr_cache=qr_cache,
                        qr_mm=qr_mm,
                    )
                else:
                    c.setFillColor(colors.black)
                    c.rect(cx, cy, card, card, fill=1, stroke=0)

        draw_cut_grid(c, x0, y0, card, rows, cols, front_cut_color, cut_lw)
        c.showPage()

        # ---------- BACK ----------
        bg_path = pick_bg_for_sheet(bg_files, si)
        key = str(bg_path.resolve())
        if key not in back_bg_cache:
            back_bg_cache[key] = ImageReader(str(bg_path))
        draw_image_cover_clipped(c, back_bg_cache[key], x0, y0, grid_w, grid_h)

        for r in range(rows):
            for col in range(cols):
                i_front = r * cols + col
                i_back = back_idx_map[i_front]
                card_data = sheet_cards[i_back]

                cx = x0 + col * card
                cy = y0 + (rows - 1 - r) * card

                title = safe_str(card_data.get("title_display") or card_data.get("title_canon") or "")
                artists = safe_str(card_data.get("artists_display") or card_data.get("artists_canon") or "")
                year = card_data.get("year", "")
                expansion = safe_str(card_data.get("expansion_code") or "I")
                owners = parse_owners(card_data)

                if title or artists or safe_str(year):
                    draw_back_text_only(
                        c, cx, cy, card, card,
                        title=title,
                        artists=artists,
                        year=year,
                        owners=owners,
                        expansion_code=expansion,
                        fonts=fonts,
                    )

        draw_cut_grid(c, x0, y0, card, rows, cols, back_cut_color, cut_lw)
        c.showPage()

        if si == 1 or si % 10 == 0 or si == total_sheets:
            pct = (si / total_sheets) * 100 if total_sheets else 100
            print(f"[{out_pdf.name}] sheet {si}/{total_sheets} ({pct:.1f}%)", flush=True)

    c.save()
    print(f"[{out_pdf.name}] OK -> {out_pdf}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--deck", default="pipeline/data/processed/deck_I.csv")
    ap.add_argument("--out-dir", default="pipeline/reports")
    ap.add_argument("--card-mm", type=float, default=65.0)
    ap.add_argument("--qr-mm", type=float, default=25.0, help="Tamaño del QR en mm (lado)")
    ap.add_argument("--front-bg", default="pipeline/cards/assets/back_bg.png")
    ap.add_argument("--back-bg-dir", default="pipeline/cards/assets/back_gradients")
    ap.add_argument("--max-sheets", type=int, default=None, help="Test: limita nº de sheets (cada sheet = 2 páginas)")
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
    front_bg = Path(args.front_bg)
    back_bg_dir = Path(args.back_bg_dir)

    variants = [
        ("4x3_short", 4, 3, "flip-short"),
        ("4x3_long", 4, 3, "flip-long"),
        ("3x3_short", 3, 3, "flip-short"),
        ("3x3_long", 3, 3, "flip-long"),
        ("4x3_match", 4, 3, "match"),
        ("3x3_match", 3, 3, "match"),
    ]

    if args.only:
        wanted = {s.strip().lower() for s in args.only.split(",") if s.strip()}
        variants = [v for v in variants if v[0].lower() in wanted]
        if not variants:
            raise SystemExit("Opciones --only: 4x3_short,4x3_long,3x3_short,3x3_long,4x3_match,3x3_match")

    for name, rows, cols, mode in variants:
        out_pdf = out_dir / f"print_{name}.pdf"
        generate_pdf(
            out_pdf=out_pdf,
            cards=cards,
            rows=rows,
            cols=cols,
            card_mm=args.card_mm,
            qr_mm=args.qr_mm,
            front_bg_path=front_bg,
            back_bg_dir=back_bg_dir,
            mode=mode,
            max_sheets=args.max_sheets,
        )


if __name__ == "__main__":
    main()

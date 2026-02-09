# -*- coding: utf-8 -*-
"""
GITSTER — Render de PDFs de impresión (A4) con FRONTS y BACKS en páginas alternas.

FRONT
- Fondo a sangre con imagen (back_bg.png)
- QR centrado, tamaño configurable (por defecto 25mm)
- Por defecto: QR clásico negro sobre blanco (máxima fiabilidad de escaneo)
- Opcional (si tienes PIL): QR “sin fondo blanco” (transparente) + underlay blanco muy sutil (alpha) para ayudar al escaneo

BACK (estilo HITSTER)
- Fondo neón con degradado suave tipo “imagen B”
  - Si PIL+numpy: PNG generado on-the-fly (mejor calidad)
  - Si NO PIL: degradado nativo (ReportLab) con muchas bandas + vignette (sin deps)
- Texto negro
- Layout centrado:
    1) title_display (hasta 2 líneas)
    2) year grande
    3) artists_display (múltiples artistas)
    4) footer en cursiva: (owners_sorted) - Exp {expansion}

PRINT SHEETS (A4)
- 6 PDFs: 4x3_short, 4x3_long, 3x3_short, 3x3_long, 4x3_match, 3x3_match
- Doble cara: short/long según “flip short/long edge”; match para validar a contraluz
- Separación mínima: solo líneas finas de corte (rejilla única por página)
"""
from __future__ import annotations

import argparse
import hashlib
import random
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
# Optional: PIL + numpy (mejor degradado y QR transparente)
# -----------------------------
try:
    import numpy as np  # type: ignore
    from PIL import Image, ImageFilter  # type: ignore
    _PIL_OK = True
except Exception:
    _PIL_OK = False


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
    """
    Estrategia robusta:
    - Si existen TTF en pipeline/cards/assets/fonts/, se registran y se usan.
    - Si no, fallback a Helvetica.

    Soporta (por este orden):
      - Inter-Regular.ttf / Inter-Bold.ttf / Inter-Italic.ttf / Inter-BoldItalic.ttf
      - Montserrat-Regular.ttf / Montserrat-Bold.ttf / Montserrat-Italic.ttf / Montserrat-BoldItalic.ttf
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
QR_BORDER_MODULES = 2


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


def make_qr_transparent_imagereader_cached(
    cache: Dict[str, ImageReader], data: str, border: int = QR_BORDER_MODULES
) -> ImageReader:
    """
    Requiere PIL. Convierte blancos a alpha=0 (QR sin fondo).
    """
    key = f"{data}::transparent"
    if key in cache:
        return cache[key]
    if not _PIL_OK:
        return make_qr_imagereader_cached(cache, data, border=border)

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
    qr_img = qr_img.convert("RGBA")

    # blancos -> transparentes (umbral alto)
    pix = qr_img.load()
    w, h = qr_img.size
    for j in range(h):
        for i in range(w):
            r, g, b, a = pix[i, j]
            if r > 250 and g > 250 and b > 250:
                pix[i, j] = (255, 255, 255, 0)
            else:
                pix[i, j] = (0, 0, 0, 255)

    buf = BytesIO()
    qr_img.save(buf, format="PNG")
    buf.seek(0)
    img = ImageReader(buf)
    cache[key] = img
    return img


# -----------------------------
# Color / gradients
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


def _hex_to_rgb01(hex_color: str) -> Tuple[float, float, float]:
    s = hex_color.strip().lstrip("#")
    if len(s) == 3:
        s = "".join([ch * 2 for ch in s])
    r = int(s[0:2], 16) / 255.0
    g = int(s[2:4], 16) / 255.0
    b = int(s[4:6], 16) / 255.0
    return r, g, b


def _rgb01_to_color(r: float, g: float, b: float) -> colors.Color:
    r = max(0.0, min(1.0, r))
    g = max(0.0, min(1.0, g))
    b = max(0.0, min(1.0, b))
    return colors.Color(r, g, b)


def _mix(a: Tuple[float, float, float], b: Tuple[float, float, float], t: float) -> Tuple[float, float, float]:
    t = max(0.0, min(1.0, t))
    return (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t, a[2] + (b[2] - a[2]) * t)


def _mul(rgb: Tuple[float, float, float], k: float) -> Tuple[float, float, float]:
    return (rgb[0] * k, rgb[1] * k, rgb[2] * k)


def make_gradient_bg_reader_cached(
    cache: Dict[str, ImageReader],
    base_hex: str,
    size_px: int = 720,
) -> Optional[ImageReader]:
    """
    Degradado de alta calidad (requiere PIL+numpy). Si no, devuelve None.
    """
    if not _PIL_OK:
        return None
    key = f"{base_hex}|{size_px}"
    if key in cache:
        return cache[key]

    r, g, b = _hex_to_rgb01(base_hex)
    base = np.array([r, g, b], dtype=np.float32)

    N = size_px
    yy, xx = np.mgrid[0:N, 0:N].astype(np.float32)
    x = xx / (N - 1)
    y = yy / (N - 1)

    # más luz arriba/izquierda, más oscuro abajo/derecha
    t = 0.30 * x + 1.00 * y
    shade = 1.18 - 0.32 * t

    # vignette suave
    r2 = (x - 0.5) ** 2 + (y - 0.5) ** 2
    vign = 1.0 - 0.28 * (r2 ** 0.7)

    img = base[None, None, :] * shade[:, :, None] * vign[:, :, None]

    # dither determinista
    seed = int(hashlib.md5(base_hex.encode("utf-8")).hexdigest()[:8], 16)
    rng = np.random.default_rng(seed)
    noise = rng.normal(0.0, 0.012, size=(N, N, 1)).astype(np.float32)
    img = img + noise

    img = np.clip(img, 0.0, 1.0)
    arr = (img * 255.0).astype(np.uint8)

    pil = Image.fromarray(arr, mode="RGB")
    pil = pil.filter(ImageFilter.GaussianBlur(radius=0.6))

    buf = BytesIO()
    pil.save(buf, format="PNG")
    buf.seek(0)
    reader = ImageReader(buf)
    cache[key] = reader
    return reader


def draw_gradient_bg_native(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    base_hex: str,
    seed_key: str,
) -> None:
    """
    Fallback sin PIL:
    - Degradado por muchas bandas horizontales (muy finas)
    - Vignette con alpha si existe (setFillAlpha)
    - Micro dither (puntos) para romper banding
    """
    base = _hex_to_rgb01(base_hex)
    top = _mul(base, 1.18)
    bottom = _mul(base, 0.88)

    steps = 240
    dh = h / steps

    for i in range(steps):
        t = i / (steps - 1)
        rgb = _mix(top, bottom, t)
        c.setFillColor(_rgb01_to_color(*rgb))
        c.rect(x, y + i * dh, w, dh + 0.5, fill=1, stroke=0)

    has_alpha = hasattr(c, "setFillAlpha")
    if has_alpha:
        bands = 26
        band_h = h * 0.16 / bands
        for i in range(bands):
            a = 0.20 * (1 - i / (bands - 1))
            c.setFillAlpha(a)
            c.setFillColor(colors.black)
            c.rect(x, y + h - (i + 1) * band_h, w, band_h + 0.5, fill=1, stroke=0)

        for i in range(bands):
            a = 0.26 * (1 - i / (bands - 1))
            c.setFillAlpha(a)
            c.setFillColor(colors.black)
            c.rect(x, y + i * band_h, w, band_h + 0.5, fill=1, stroke=0)

        bands_w = 22
        band_w = w * 0.14 / bands_w
        for i in range(bands_w):
            a = 0.16 * (1 - i / (bands_w - 1))
            c.setFillAlpha(a)
            c.setFillColor(colors.black)
            c.rect(x + i * band_w, y, band_w + 0.5, h, fill=1, stroke=0)
            c.rect(x + w - (i + 1) * band_w, y, band_w + 0.5, h, fill=1, stroke=0)

        c.setFillAlpha(1.0)

    # micro dither
    rnd_seed = int(hashlib.md5(seed_key.encode("utf-8")).hexdigest()[:8], 16)
    rnd = random.Random(rnd_seed)
    dots = 48
    if has_alpha:
        c.setFillAlpha(0.06)
    for _ in range(dots):
        px = x + rnd.random() * w
        py = y + rnd.random() * h
        s = 0.9
        c.setFillColor(colors.white if rnd.random() < 0.5 else colors.black)
        c.rect(px, py, s, s, fill=1, stroke=0)
    if has_alpha:
        c.setFillAlpha(1.0)


# -----------------------------
# Drawing (cards + cut grid)
# -----------------------------
def draw_full_bleed_image(
    c: canvas.Canvas, img_reader: ImageReader, x: float, y: float, w: float, h: float
) -> None:
    c.drawImage(img_reader, x, y, w, h, mask="auto")


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
    """
    Rejilla única por página (evita dobles líneas).
    """
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
    qr_transparent: bool,
    qr_underlay_alpha: float,
) -> None:
    # Fondo a sangre
    if bg_reader is not None:
        draw_full_bleed_image(c, bg_reader, x, y, w, h)
    else:
        c.setFillColor(colors.black)
        c.rect(x, y, w, h, fill=1, stroke=0)

    qr_size = mm_to_pt(qr_mm)
    qr_size = min(qr_size, min(w, h) * 0.85)
    qr_x = x + (w - qr_size) / 2
    qr_y = y + (h - qr_size) / 2

    # Underlay sutil (solo si QR transparente y el canvas soporta alpha)
    if qr_transparent and qr_underlay_alpha > 0 and hasattr(c, "setFillAlpha"):
        c.saveState()
        c.setFillAlpha(max(0.0, min(1.0, qr_underlay_alpha)))
        c.setFillColor(colors.white)
        pad = qr_size * 0.06
        c.rect(qr_x - pad, qr_y - pad, qr_size + 2 * pad, qr_size + 2 * pad, fill=1, stroke=0)
        c.restoreState()

    # QR
    if qr_transparent and _PIL_OK:
        qr_reader = make_qr_transparent_imagereader_cached(qr_cache, spotify_url, border=QR_BORDER_MODULES)
    else:
        qr_reader = make_qr_imagereader_cached(qr_cache, spotify_url, border=QR_BORDER_MODULES)

    c.drawImage(qr_reader, qr_x, qr_y, qr_size, qr_size, mask="auto")


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
    bg_cache: Dict[str, ImageReader],
) -> None:
    key = f"{title}|{artists}|{year}|{expansion_code}"
    base_hex = stable_palette_pick(key, NEON_BG_PALETTE)

    bg_reader = make_gradient_bg_reader_cached(bg_cache, base_hex, size_px=720)
    if bg_reader is not None:
        draw_full_bleed_image(c, bg_reader, x, y, w, h)
    else:
        draw_gradient_bg_native(c, x, y, w, h, base_hex, seed_key=key)

    # Texto negro
    c.setFillColor(colors.black)
    pad_x = w * 0.08
    max_w = w - 2 * pad_x
    x_center = x + w / 2

    # 1) Título (comedido; un poco > artistas)
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

    # 2) Año
    year_str = normalize_year(year)
    year_size = 64 if year_str != "—" else 52
    c.setFont(fonts["bold"], year_size)
    y_center_target = y + h * 0.52
    y_baseline = y_center_target - (0.35 * year_size)
    c.drawCentredString(x_center, y_baseline, year_str)

    # 3) Artistas
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

    # 4) Footer cursiva
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
    bg_path: Path,
    mode: str,
    max_sheets: Optional[int],
    qr_transparent: bool,
    qr_underlay_alpha: float,
) -> None:
    out_pdf.parent.mkdir(parents=True, exist_ok=True)

    page_w, page_h = mm_to_pt(210), mm_to_pt(297)  # A4 portrait
    card = mm_to_pt(card_mm)

    x0, y0 = compute_grid(page_w, page_h, card, rows, cols)

    fonts = try_register_fonts(Path("pipeline/cards/assets/fonts"))
    bg_reader = ImageReader(str(bg_path)) if bg_path.exists() else None
    qr_cache: Dict[str, ImageReader] = {}
    bg_cache: Dict[str, ImageReader] = {}

    per_sheet = rows * cols
    sheets = chunk(cards, per_sheet)
    if max_sheets is not None:
        sheets = sheets[:max_sheets]

    total_sheets = len(sheets)
    total_pages = total_sheets * 2

    print(
        f"[{out_pdf.name}] {total_sheets} sheets -> {total_pages} páginas "
        f"(modo={mode}, grid={rows}x{cols}, qr={qr_mm:.1f}mm, "
        f"gradient={'PIL' if _PIL_OK else 'native'}, "
        f"qr_transparent={'on' if (qr_transparent and _PIL_OK) else 'off'})",
        flush=True,
    )

    c = canvas.Canvas(str(out_pdf), pagesize=(page_w, page_h))

    front_cut_color = colors.white
    back_cut_color = colors.black
    cut_lw = 0.6

    back_idx_map = order_back_indices(rows, cols, mode)

    for si, sheet_cards in enumerate(sheets, start=1):
        if len(sheet_cards) < per_sheet:
            sheet_cards = sheet_cards + ([{}] * (per_sheet - len(sheet_cards)))

        # ---------------- FRONT ----------------
        for r in range(rows):
            for col in range(cols):
                i = r * cols + col
                card_data = sheet_cards[i]

                cx = x0 + col * card
                cy = y0 + (rows - 1 - r) * card

                spotify_url = safe_str(card_data.get("spotify_url") or "")
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
                        qr_mm=qr_mm,
                        qr_transparent=qr_transparent,
                        qr_underlay_alpha=qr_underlay_alpha,
                    )
                else:
                    c.setFillColor(colors.black)
                    c.rect(cx, cy, card, card, fill=1, stroke=0)

        draw_cut_grid(c, x0, y0, card, rows, cols, front_cut_color, cut_lw)
        c.showPage()

        # ---------------- BACK ----------------
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
                        bg_cache=bg_cache,
                    )
                else:
                    c.setFillColor(colors.white)
                    c.rect(cx, cy, card, card, fill=1, stroke=0)

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
    ap.add_argument("--bg", default="pipeline/cards/assets/back_bg.png")
    ap.add_argument("--max-sheets", type=int, default=None, help="Test: limita nº de sheets (cada sheet = 2 páginas)")
    ap.add_argument(
        "--only",
        default="",
        help="(opcional) genera solo: 4x3_short, 4x3_long, 3x3_short, 3x3_long, 4x3_match, 3x3_match",
    )
    ap.add_argument(
        "--qr-transparent",
        action="store_true",
        help="(requiere PIL) QR sin fondo blanco (transparente).",
    )
    ap.add_argument(
        "--qr-underlay-alpha",
        type=float,
        default=0.12,
        help="Solo si --qr-transparent: alpha del underlay blanco sutil detrás del QR (0..1).",
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
            bg_path=bg_path,
            mode=mode,
            max_sheets=args.max_sheets,
            qr_transparent=args.qr_transparent,
            qr_underlay_alpha=max(0.0, min(1.0, args.qr_underlay_alpha)),
        )


if __name__ == "__main__":
    main()

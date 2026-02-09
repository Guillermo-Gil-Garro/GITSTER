# pipeline/spotify/export/parse_yourlibrary_export.py
"""
Parsea Spotify Data Export: YourLibrary.json (Liked Songs) por owner.

Input esperado (por defecto):
- pipeline/data/raw/spotify_export/<OWNER>/YourLibrary.json

Output:
- pipeline/data/processed/spotify_liked_songs_from_export_<OWNER>.csv

Columnas (mínimas):
- owner_label, title, artists, spotify_uri, track_id, spotify_url, year

Columnas adicionales si se pueden extraer:
- album_name, album_uri, album_id, album_release_date

Notas:
- El schema exacto de YourLibrary.json varía según export/versión.
  Este parser intenta encontrar claves comunes (en minúsculas y mayúsculas) y
  soporta valores como string/list/dict cuando sea posible.
"""
import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd

BASE_DIR = Path("pipeline/data/raw/spotify_export")
OUT_DIR = Path("pipeline/data/processed")

TRACK_URI_RE = re.compile(r"^spotify:track:([A-Za-z0-9]+)$")
ALBUM_URI_RE = re.compile(r"^spotify:album:([A-Za-z0-9]+)$")
YEAR_RE = re.compile(r"^(\d{4})")


def _as_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, (int, float)):
        # evitar 1978.0
        s = str(x)
        return s.split(".")[0]
    return str(x).strip()


def _get_any(d: Dict[str, Any], keys: Iterable[str]) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    # fallback case-insensitive
    lower = {str(k).lower(): k for k in d.keys()}
    for k in keys:
        lk = str(k).lower()
        if lk in lower:
            return d.get(lower[lk])
    return None


def _join_artists(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, list):
        out = []
        for a in val:
            if isinstance(a, dict):
                name = _as_str(_get_any(a, ["name", "artist", "artistName"]))
            else:
                name = _as_str(a)
            if name:
                out.append(name)
        return ", ".join(out).strip()
    if isinstance(val, dict):
        return _as_str(_get_any(val, ["name", "artist", "artistName"]))
    return _as_str(val)


def _extract_track_fields(t: Dict[str, Any]) -> Tuple[str, str, str]:
    title = _as_str(_get_any(t, ["track", "title", "trackName", "name"]))
    # a veces viene como dict {"name": "..."}
    if isinstance(_get_any(t, ["track", "title", "trackName", "name"]), dict):
        title = _as_str(_get_any(_get_any(t, ["track", "title", "trackName", "name"]), ["name", "title", "track"]))
    artists = _join_artists(_get_any(t, ["artist", "artists", "artistName", "artistNames"]))
    uri = _as_str(_get_any(t, ["uri", "spotify_uri", "spotifyUri", "track_uri", "trackUri"]))
    # a veces viene como url
    if not uri and _as_str(_get_any(t, ["spotify_url", "spotifyUrl", "url"])).startswith("https://open.spotify.com/track/"):
        url = _as_str(_get_any(t, ["spotify_url", "spotifyUrl", "url"]))
        tid = url.rstrip("/").split("/")[-1].split("?")[0]
        uri = f"spotify:track:{tid}"
    return title, artists, uri


def _extract_album_fields(t: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Devuelve (album_name, album_uri, album_id, album_release_date)
    """
    album = _get_any(t, ["album", "albumName", "album_name", "albumTitle"])
    album_name = ""
    album_uri = ""
    album_id = ""
    album_release_date = _as_str(_get_any(t, ["album_release_date", "albumReleaseDate", "release_date", "releaseDate"]))

    if isinstance(album, dict):
        album_name = _as_str(_get_any(album, ["name", "album", "title"]))
        album_uri = _as_str(_get_any(album, ["uri", "spotify_uri", "spotifyUri"]))
        album_id = _as_str(_get_any(album, ["id", "album_id", "albumId"]))
    elif isinstance(album, str):
        album_name = _as_str(album)

    # algunos exports ponen album_uri/album_id sueltos
    if not album_uri:
        album_uri = _as_str(_get_any(t, ["album_uri", "albumUri"]))
    if not album_id:
        album_id = _as_str(_get_any(t, ["album_id", "albumId"]))

    # derivar album_id desde uri
    if album_uri and not album_id:
        m = ALBUM_URI_RE.match(album_uri)
        if m:
            album_id = m.group(1)

    return album_name, album_uri, album_id, album_release_date


def _extract_year(t: Dict[str, Any], album_release_date: str) -> str:
    y = _as_str(_get_any(t, ["year", "Year"]))
    if y and YEAR_RE.match(y):
        return YEAR_RE.match(y).group(1)

    # intento por release date
    for candidate in [_as_str(_get_any(t, ["release_date", "releaseDate"])), album_release_date]:
        if candidate:
            m = YEAR_RE.match(candidate)
            if m:
                return m.group(1)

    return ""


def parse_one(owner: str) -> Path:
    in_path = BASE_DIR / owner / "YourLibrary.json"
    if not in_path.exists():
        raise FileNotFoundError(f"No existe: {in_path}")

    data = json.loads(in_path.read_text(encoding="utf-8", errors="ignore"))
    tracks = data.get("tracks") or data.get("Tracks") or data.get("items") or []

    rows = []
    for t in tracks:
        if not isinstance(t, dict):
            continue

        title, artists, uri = _extract_track_fields(t)
        if not title or not artists:
            continue

        track_id = ""
        if uri:
            m = TRACK_URI_RE.match(uri)
            if m:
                track_id = m.group(1)

        url = f"https://open.spotify.com/track/{track_id}" if track_id else ""

        album_name, album_uri, album_id, album_release_date = _extract_album_fields(t)
        year = _extract_year(t, album_release_date)

        rows.append(
            {
                "owner_label": owner,
                "title": title,
                "artists": artists,
                "spotify_uri": uri,
                "track_id": track_id,
                "spotify_url": url,
                "album_name": album_name,
                "album_uri": album_uri,
                "album_id": album_id,
                "album_release_date": album_release_date,
                "year": year,  # si no se puede, queda ''
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit(f"No se han podido extraer filas desde {in_path}. Revisa el schema del JSON.")

    # Deduplicar preferentemente por track_id si existe; si no, por (spotify_uri,title,artists)
    if "track_id" in df.columns and df["track_id"].astype(str).str.strip().ne("").any():
        df = df.drop_duplicates(subset=["track_id"])
    else:
        df = df.drop_duplicates(subset=["spotify_uri", "title", "artists"])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"spotify_liked_songs_from_export_{owner}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"OK: {len(df)} filas → {out_path}")
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--owner", type=str, help="Nombre de carpeta owner (ej. Guille, Luks)")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Procesa todos los owners bajo raw/spotify_export/<OWNER>/YourLibrary.json",
    )
    args = ap.parse_args()

    if args.all:
        owners = [
            p.name
            for p in BASE_DIR.iterdir()
            if p.is_dir() and (p / "YourLibrary.json").exists()
        ]
        if not owners:
            raise SystemExit(
                "No hay owners con YourLibrary.json en pipeline/data/raw/spotify_export/<OWNER>/YourLibrary.json"
            )
        for o in owners:
            parse_one(o)
        return

    owner = args.owner or "Guille"
    parse_one(owner)


if __name__ == "__main__":
    main()

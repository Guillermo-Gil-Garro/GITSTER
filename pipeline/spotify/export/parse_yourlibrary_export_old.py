# pipeline/spotify/export/parse_yourlibrary_export.py
import json
import argparse
from pathlib import Path
import pandas as pd

BASE_DIR = Path("pipeline/data/raw/spotify_export")
OUT_DIR = Path("pipeline/data/processed")


def parse_one(owner: str) -> Path:
    in_path = BASE_DIR / owner / "YourLibrary.json"
    if not in_path.exists():
        raise FileNotFoundError(f"No existe: {in_path}")

    data = json.loads(in_path.read_text(encoding="utf-8"))
    tracks = data.get("tracks") or data.get("Tracks") or []

    rows = []
    for t in tracks:
        title = (t.get("track") or t.get("title") or "").strip()
        artists = (t.get("artist") or t.get("artists") or "").strip()
        uri = (t.get("uri") or "").strip()
        track_id = uri.split(":")[-1] if uri.startswith("spotify:track:") else ""
        url = f"https://open.spotify.com/track/{track_id}" if track_id else ""

        if not title or not artists:
            continue

        rows.append(
            {
                "owner_label": owner,
                "title": title,
                "artists": artists,
                "spotify_uri": uri,
                "track_id": track_id,
                "spotify_url": url,
                "year": "",
            }
        )

    df = pd.DataFrame(rows).drop_duplicates(subset=["spotify_uri", "title", "artists"])
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"spotify_liked_songs_from_export__{owner}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"OK: {len(df)} filas → {out_path}")
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--owner", type=str, help="Nombre de carpeta owner (ej. Guille, Colega_1)"
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Procesa todos los owners bajo raw/spotify_export/",
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

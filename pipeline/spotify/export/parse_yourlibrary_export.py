import json
from pathlib import Path
import pandas as pd

RAW_DIR = Path("pipeline/data/raw")
OUT_PATH = Path("pipeline/data/processed/spotify_liked_songs_from_export.csv")


def find_yourlibrary_json() -> Path:
    matches = list(RAW_DIR.rglob("YourLibrary.json"))
    if not matches:
        raise FileNotFoundError(
            "No encuentro YourLibrary.json. Ponlo en pipeline/data/raw/**/YourLibrary.json"
        )
    matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return matches[0]


def uri_to_track_id(uri: str) -> str:
    return uri.split(":")[-1] if uri and uri.startswith("spotify:track:") else ""


def main():
    path = find_yourlibrary_json()
    data = json.loads(path.read_text(encoding="utf-8"))

    tracks = data.get("tracks") or data.get("Tracks") or []
    rows = []
    for t in tracks:
        title = (t.get("track") or t.get("title") or "").strip()
        artists = (t.get("artist") or t.get("artists") or "").strip()
        uri = (t.get("uri") or "").strip()

        if not title or not artists:
            continue

        track_id = uri_to_track_id(uri)
        rows.append(
            {
                "source_type": "liked_songs_export",
                "source_name": "Spotify YourLibrary.json",
                "title": title,
                "artists": artists,
                "spotify_uri": uri,
                "track_id": track_id,
                "spotify_url": (
                    f"https://open.spotify.com/track/{track_id}" if track_id else ""
                ),
                "year": "",  # el export no trae año
            }
        )

    df = pd.DataFrame(rows).drop_duplicates(subset=["spotify_uri", "title", "artists"])
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"OK: {len(df)} filas → {OUT_PATH}")


if __name__ == "__main__":
    main()

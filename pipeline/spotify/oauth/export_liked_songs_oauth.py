import os
import pandas as pd
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth

load_dotenv()

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")

if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
    raise SystemExit("Faltan variables en .env (CLIENT_ID/CLIENT_SECRET/REDIRECT_URI).")

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="user-library-read",
        open_browser=True,
        cache_path=".cache-spotify-token",
    )
)

rows = []
limit, offset = 50, 0

while True:
    page = sp.current_user_saved_tracks(limit=limit, offset=offset)
    for it in page.get("items", []):
        t = (it or {}).get("track")
        if not t:
            continue

        track_id = t.get("id") or ""
        album = t.get("album") or {}
        release_date = album.get("release_date") or ""

        rows.append(
            {
                "source_type": "liked_songs",
                "source_name": "Spotify Liked Songs",
                "added_at": it.get("added_at") or "",
                "track_id": track_id,
                "spotify_uri": t.get("uri") or "",
                "spotify_url": (
                    f"https://open.spotify.com/track/{track_id}" if track_id else ""
                ),
                "title": t.get("name") or "",
                "artists": "; ".join(
                    a.get("name", "") for a in (t.get("artists") or []) if a.get("name")
                ),
                "album_name": album.get("name") or "",
                "album_type": album.get("album_type") or "",
                "release_date": release_date,
                "release_date_precision": album.get("release_date_precision") or "",
                "year": (release_date[:4] if release_date else ""),
                "preview_url": t.get("preview_url") or "",
            }
        )

    if not page.get("next"):
        break
    offset += limit

df = pd.DataFrame(rows).fillna("")
out_path = os.path.join("pipeline", "data", "processed", "spotify_liked_songs.csv")
df.to_csv(out_path, index=False, encoding="utf-8")
print(f"OK: {len(df)} filas → {out_path}")

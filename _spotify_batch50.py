from dotenv import load_dotenv
import os, base64, requests, sys
from pathlib import Path
import pandas as pd

load_dotenv()

cid = os.getenv("SPOTIFY_CLIENT_ID")
sec = os.getenv("SPOTIFY_CLIENT_SECRET")

print("CID", "OK" if cid else "MISSING")
print("SECRET", "OK" if sec else "MISSING")
if not cid or not sec:
    sys.exit(2)

csv_path = Path("pipeline/data/processed/spotify_liked_songs_from_export_Guille.csv")
df = pd.read_csv(csv_path)
ids = df["track_id"].astype(str).str.strip().head(50).tolist()

bad = [i for i in ids if len(i)!=22 or not i.isalnum()]
print("IDS_50", len(ids), "BAD", len(bad))
if bad:
    print("BAD_EXAMPLES", bad[:10])

auth = base64.b64encode((cid + ":" + sec).encode("utf-8")).decode("utf-8")
r = requests.post(
    "https://accounts.spotify.com/api/token",
    data={"grant_type": "client_credentials"},
    headers={"Authorization": f"Basic {auth}"},
    timeout=30,
)
print("TOKEN_STATUS", r.status_code)
if not r.ok:
    print(r.text[:500]); sys.exit(3)

tok = r.json().get("access_token")
tr = requests.get(
    "https://api.spotify.com/v1/tracks",
    params={"ids": ",".join(ids)},
    headers={"Authorization": f"Bearer {tok}"},
    timeout=30,
)
print("BATCH_STATUS", tr.status_code)
print(tr.text[:500])

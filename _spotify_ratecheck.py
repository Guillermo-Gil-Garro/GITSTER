from dotenv import load_dotenv
import os, base64, requests, time
import pandas as pd
from pathlib import Path

load_dotenv()
cid=os.getenv("SPOTIFY_CLIENT_ID")
sec=os.getenv("SPOTIFY_CLIENT_SECRET")
print("CID", "OK" if cid else "MISSING")
print("SECRET", "OK" if sec else "MISSING")

auth=base64.b64encode((cid+":"+sec).encode()).decode()
r=requests.post("https://accounts.spotify.com/api/token",
                data={"grant_type":"client_credentials"},
                headers={"Authorization":f"Basic {auth}"},
                timeout=30)
print("TOKEN_STATUS", r.status_code)
if not r.ok:
    print(r.text[:300])
    raise SystemExit(1)

tok=r.json()["access_token"]
df=pd.read_csv(Path("pipeline/data/processed/spotify_liked_songs_from_export_Guille.csv"))
ids=df["track_id"].astype(str).str.strip().head(5).tolist()

s=requests.Session()
for i,tid in enumerate(ids,1):
    t0=time.time()
    tr=s.get(f"https://api.spotify.com/v1/tracks/{tid}",
             params={"market":"ES"},
             headers={"Authorization":f"Bearer {tok}"},
             timeout=30)
    ra=tr.headers.get("Retry-After")
    print(f"{i}/5 STATUS", tr.status_code, "Retry-After", ra, "secs", "t", round(time.time()-t0,2))
    time.sleep(0.4)

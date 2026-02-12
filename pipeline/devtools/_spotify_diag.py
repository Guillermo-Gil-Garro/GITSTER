from dotenv import load_dotenv
import os, base64, requests, sys

load_dotenv()

cid = os.getenv("SPOTIFY_CLIENT_ID")
sec = os.getenv("SPOTIFY_CLIENT_SECRET")

print("CID", "OK" if cid else "MISSING", len(cid or ""))
print("SECRET", "OK" if sec else "MISSING", len(sec or ""))

if not cid or not sec:
    sys.exit(2)

auth = base64.b64encode((cid + ":" + sec).encode("utf-8")).decode("utf-8")

r = requests.post(
    "https://accounts.spotify.com/api/token",
    data={"grant_type": "client_credentials"},
    headers={"Authorization": f"Basic {auth}"},
    timeout=30,
)

print("TOKEN_STATUS", r.status_code)
print(r.text[:300])

if not r.ok:
    sys.exit(3)

tok = r.json().get("access_token")

tr = requests.get(
    "https://api.spotify.com/v1/tracks",
    params={"ids": "11dFghVXANMlKmJXsNCbNl"},
    headers={"Authorization": f"Bearer {tok}"},
    timeout=30,
)

print("TRACK_STATUS", tr.status_code)
print(tr.text[:300])

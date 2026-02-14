from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv


TRACK_ID_RE = re.compile(r"^[A-Za-z0-9]{22}$")
TRACK_URL_RE = re.compile(r"open\.spotify\.com/track/([^/?]+)")
TRACK_URI_RE = re.compile(r"spotify:track:([A-Za-z0-9]{22})")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


ROOT = repo_root()
PROCESSED_DIR = ROOT / "pipeline" / "data" / "processed"
CACHE_DIR = ROOT / "pipeline" / "cache"


def clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def normalize_track_id(raw: Any) -> Optional[str]:
    value = clean_str(raw)
    if not value:
        return None

    if TRACK_ID_RE.fullmatch(value):
        return value

    uri_match = TRACK_URI_RE.search(value)
    if uri_match:
        candidate = clean_str(uri_match.group(1))
        return candidate if TRACK_ID_RE.fullmatch(candidate) else None

    url_match = TRACK_URL_RE.search(value)
    if url_match:
        candidate = clean_str(url_match.group(1)).split("?")[0]
        return candidate if TRACK_ID_RE.fullmatch(candidate) else None

    return None


def split_csv_like(text: Any) -> List[str]:
    raw = clean_str(text)
    if not raw:
        return []
    items = [clean_str(part) for part in raw.split(",")]
    return [part for part in items if part]


def build_artists_display(names: List[str], artists_count: int) -> str:
    clean_names = [clean_str(name) for name in names if clean_str(name)]
    if not clean_names:
        return ""
    count = int(artists_count) if int(artists_count) > 0 else len(clean_names)
    if count <= 3 or len(clean_names) <= 3:
        return ", ".join(clean_names[: max(1, min(len(clean_names), count))])
    extra = max(0, count - 3)
    shown = clean_names[:3]
    return f"{', '.join(shown)} +{extra}" if extra > 0 else ", ".join(shown)


def parse_cache_artists(entry: Any) -> List[Dict[str, str]]:
    if not isinstance(entry, dict):
        return []

    artists = entry.get("artists")
    if isinstance(artists, list):
        out: List[Dict[str, str]] = []
        for artist in artists:
            if not isinstance(artist, dict):
                continue
            artist_id = clean_str(artist.get("id"))
            name = clean_str(artist.get("name"))
            if artist_id or name:
                out.append({"id": artist_id, "name": name})
        if out:
            return out

    artists_full_json = clean_str(entry.get("artists_full_json"))
    if artists_full_json.startswith("[") and artists_full_json.endswith("]"):
        try:
            arr = json.loads(artists_full_json)
        except Exception:
            arr = []
        if isinstance(arr, list):
            out = []
            for row in arr:
                if not isinstance(row, dict):
                    continue
                artist_id = clean_str(row.get("id"))
                name = clean_str(row.get("name"))
                if artist_id or name:
                    out.append({"id": artist_id, "name": name})
            if out:
                return out

    artists_all = split_csv_like(entry.get("artists_all") or entry.get("artists_full"))
    if artists_all:
        return [{"id": "", "name": name} for name in artists_all]

    return []


def has_valid_cache_entry(entry: Any) -> bool:
    return len(parse_cache_artists(entry)) > 0


def load_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    if isinstance(payload, dict) and isinstance(payload.get("tracks"), dict):
        raw = payload.get("tracks", {})
    elif isinstance(payload, dict):
        raw = payload
    else:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            out[str(key)] = value
    return out


def save_cache(path: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 3,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "tracks": cache,
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


class SpotifyAppClient:
    def __init__(self, client_id: str, client_secret: str, timeout: float = 30.0):
        self.client_id = clean_str(client_id)
        self.client_secret = clean_str(client_secret)
        self.timeout = float(timeout)
        self.session = requests.Session()
        self.access_token: Optional[str] = None
        self.expires_at = 0.0

    def get_spotify_app_token(self, force_refresh: bool = False) -> str:
        now = time.time()
        if (not force_refresh) and self.access_token and now < (self.expires_at - 60):
            return self.access_token

        raw = f"{self.client_id}:{self.client_secret}".encode("utf-8")
        basic = base64.b64encode(raw).decode("ascii")

        resp = self.session.post(
            "https://accounts.spotify.com/api/token",
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={"grant_type": "client_credentials"},
            timeout=self.timeout,
        )
        resp.raise_for_status()

        payload = resp.json()
        token = clean_str(payload.get("access_token"))
        expires_in = int(payload.get("expires_in", 3600) or 3600)
        if not token:
            raise RuntimeError("Spotify app token is missing")

        self.access_token = token
        self.expires_at = time.time() + max(60, expires_in)
        return token

    def fetch_tracks_batch(
        self,
        track_ids: List[str],
        market: str,
        max_retries: int,
    ) -> Dict[str, List[Dict[str, str]]]:
        ids_param = ",".join(track_ids)
        refreshed_after_401 = False
        use_market = bool(clean_str(market))

        for attempt in range(1, max_retries + 2):
            token = self.get_spotify_app_token(force_refresh=False)
            headers = {"Authorization": f"Bearer {token}"}
            params: Dict[str, str] = {"ids": ids_param}
            if use_market:
                params["market"] = clean_str(market)

            resp = self.session.get(
                "https://api.spotify.com/v1/tracks",
                params=params,
                headers=headers,
                timeout=self.timeout,
            )

            if resp.status_code == 401 and (not refreshed_after_401):
                self.get_spotify_app_token(force_refresh=True)
                refreshed_after_401 = True
                continue

            if resp.status_code == 429:
                retry_after_raw = clean_str(resp.headers.get("Retry-After", "1"))
                retry_after = int(retry_after_raw) if retry_after_raw.isdigit() else 1
                if attempt > max_retries:
                    raise RuntimeError(
                        f"429 Retry-After={retry_after}s and retry limit reached (max_retries={max_retries})"
                    )
                print(
                    f"[deck_spotify] 429 received, waiting {retry_after}s "
                    f"(attempt {attempt}/{max_retries})"
                )
                time.sleep(float(retry_after) + 0.1)
                continue

            if resp.status_code in {500, 502, 503, 504} and attempt <= max_retries:
                backoff = min(20.0, 0.5 * (2 ** (attempt - 1)))
                print(
                    f"[deck_spotify] transient status={resp.status_code}, retry in {backoff:.1f}s "
                    f"(attempt {attempt}/{max_retries})"
                )
                time.sleep(backoff)
                continue

            if resp.status_code == 403 and use_market:
                print("[deck_spotify] 403 with market, retrying batch without market parameter")
                use_market = False
                continue

            if resp.status_code != 200:
                body = clean_str(resp.text)[:500]
                raise RuntimeError(f"Spotify /v1/tracks failed status={resp.status_code} body={body}")

            payload = resp.json()
            tracks = payload.get("tracks") or []
            out: Dict[str, List[Dict[str, str]]] = {}
            for track_obj in tracks:
                if not isinstance(track_obj, dict):
                    continue
                track_id = clean_str(track_obj.get("id"))
                if not track_id:
                    continue
                artists: List[Dict[str, str]] = []
                for artist_obj in track_obj.get("artists") or []:
                    if not isinstance(artist_obj, dict):
                        continue
                    artist_id = clean_str(artist_obj.get("id"))
                    artist_name = clean_str(artist_obj.get("name"))
                    if artist_id or artist_name:
                        artists.append({"id": artist_id, "name": artist_name})
                out[track_id] = artists
            return out

        raise RuntimeError("Spotify batch retries exhausted")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Enrich final deck with Spotify artists_all/artists_display.")
    ap.add_argument("--expansion", default="I")
    ap.add_argument("--deck-path", default=None, help="Default: pipeline/data/processed/deck_{EXP}.csv")
    ap.add_argument("--deck-json-path", default=None, help="Default: deck CSV path with .json extension.")
    ap.add_argument(
        "--cache-path",
        default=str(CACHE_DIR / "spotify_tracks_cache.json"),
        help="JSON cache path.",
    )
    ap.add_argument("--spotify-max-requests", type=int, default=20, help="Budget max requests (batches of 50).")
    ap.add_argument("--market", default="ES")
    ap.add_argument("--throttle", type=float, default=0.2, help="Extra sleep between batch requests.")
    ap.add_argument("--max-retries", type=int, default=5, help="Retries per batch for 429/5xx.")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    expansion = clean_str(args.expansion) or "I"

    deck_path = Path(args.deck_path) if args.deck_path else (PROCESSED_DIR / f"deck_{expansion}.csv")
    deck_json_path = Path(args.deck_json_path) if args.deck_json_path else deck_path.with_suffix(".json")
    cache_path = Path(args.cache_path)

    if not deck_path.exists():
        raise FileNotFoundError(f"Deck CSV not found: {deck_path}")

    df = pd.read_csv(deck_path).fillna("")
    if df.empty:
        raise SystemExit(f"[deck_spotify] deck is empty: {deck_path}")

    row_track_ids: List[Optional[str]] = []
    for row in df.to_dict(orient="records"):
        track_id = (
            normalize_track_id(row.get("spotify_track_id"))
            or normalize_track_id(row.get("track_id"))
            or normalize_track_id(row.get("spotify_url"))
            or normalize_track_id(row.get("spotify_uri"))
        )
        row_track_ids.append(track_id)

    unique_track_ids: List[str] = []
    seen: set[str] = set()
    for track_id in row_track_ids:
        if track_id and track_id not in seen:
            seen.add(track_id)
            unique_track_ids.append(track_id)

    cache = load_cache(cache_path)
    cached_track_artists: Dict[str, List[Dict[str, str]]] = {}
    missing_ids: List[str] = []
    cache_hits = 0

    for track_id in unique_track_ids:
        entry = cache.get(track_id, {})
        if has_valid_cache_entry(entry):
            cached_track_artists[track_id] = parse_cache_artists(entry)
            cache_hits += 1
        else:
            missing_ids.append(track_id)

    num_batches = int(math.ceil(len(missing_ids) / 50.0)) if missing_ids else 0
    budget = int(args.spotify_max_requests)

    print(f"[deck_spotify] unique_track_ids={len(unique_track_ids)}")
    print(f"[deck_spotify] cache_hits={cache_hits}")
    print(f"[deck_spotify] missing_ids={len(missing_ids)}")
    print(f"[deck_spotify] batches_needed={num_batches} budget={budget}")

    if num_batches > budget:
        raise SystemExit(
            "[deck_spotify] abort: Spotify request budget exceeded "
            f"(needed={num_batches}, budget={budget}). "
            "Increase with --spotify-max-requests."
        )

    if missing_ids:
        load_dotenv()
        client_id = clean_str(os.getenv("SPOTIFY_CLIENT_ID"))
        client_secret = clean_str(os.getenv("SPOTIFY_CLIENT_SECRET"))
        if not client_id or not client_secret:
            raise SystemExit("Missing SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET in environment/.env")

        client = SpotifyAppClient(client_id=client_id, client_secret=client_secret)
        for batch_idx in range(0, len(missing_ids), 50):
            batch = missing_ids[batch_idx : batch_idx + 50]
            batch_no = (batch_idx // 50) + 1
            fetched_at = datetime.now().isoformat(timespec="seconds")
            try:
                artists_map = client.fetch_tracks_batch(
                    track_ids=batch,
                    market=clean_str(args.market) or "ES",
                    max_retries=max(0, int(args.max_retries)),
                )
            except Exception as exc:
                err = clean_str(exc)
                print(f"[deck_spotify] warning: batch {batch_no}/{num_batches} failed: {err}")
                for track_id in batch:
                    entry = cache.get(track_id, {}) if isinstance(cache.get(track_id), dict) else {}
                    if not isinstance(entry, dict):
                        entry = {}
                    entry["_error"] = err[:300]
                    entry["fetched_at"] = fetched_at
                    entry["source"] = "v1/tracks_batch"
                    cache[track_id] = entry
                save_cache(cache_path, cache)
                if float(args.throttle) > 0:
                    time.sleep(float(args.throttle))
                continue

            for track_id in batch:
                artists = artists_map.get(track_id, [])
                entry = cache.get(track_id, {}) if isinstance(cache.get(track_id), dict) else {}
                if not isinstance(entry, dict):
                    entry = {}
                entry["artists"] = artists
                entry["fetched_at"] = fetched_at
                entry["source"] = "v1/tracks_batch"
                cache[track_id] = entry
                cached_track_artists[track_id] = artists

            save_cache(cache_path, cache)
            print(f"[deck_spotify] fetched batch {batch_no}/{num_batches}")
            if float(args.throttle) > 0:
                time.sleep(float(args.throttle))

    updated_records: List[Dict[str, Any]] = []
    for row, track_id in zip(df.to_dict(orient="records"), row_track_ids):
        row_out = dict(row)

        if track_id:
            row_out["spotify_track_id"] = track_id

        artists_objs = cached_track_artists.get(track_id or "", [])
        if not artists_objs:
            artists_objs = parse_cache_artists(cache.get(track_id or "", {}))

        names = [clean_str(item.get("name")) for item in artists_objs if clean_str(item.get("name"))]
        ids = [clean_str(item.get("id")) for item in artists_objs if clean_str(item.get("id"))]

        if not names:
            names = split_csv_like(row_out.get("artists_all")) or split_csv_like(row_out.get("artists_canon"))
        if not ids:
            ids = split_csv_like(row_out.get("artists_ids"))

        artists_count = len(names)
        if artists_count <= 0:
            artists_count = parse_int(row_out.get("artists_count"), default=0)
            if artists_count <= 0:
                artists_count = len(names)

        row_out["artists_all"] = ", ".join(names)
        row_out["artists_count"] = int(artists_count)
        row_out["artists_ids"] = ", ".join(ids)

        computed_display = build_artists_display(names, artists_count) if names else ""
        if computed_display:
            row_out["artists_display"] = computed_display
        else:
            row_out["artists_display"] = clean_str(row_out.get("artists_display")) or clean_str(row_out.get("artists_canon"))

        updated_records.append(row_out)

    out_df = pd.DataFrame(updated_records)
    original_cols = list(df.columns)
    appended_cols = ["spotify_track_id", "artists_all", "artists_count", "artists_ids", "artists_display"]
    final_cols = original_cols + [col for col in appended_cols if col not in original_cols]
    final_cols = final_cols + [col for col in out_df.columns if col not in final_cols]
    out_df = out_df[final_cols]

    deck_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(deck_path, index=False, encoding="utf-8")

    deck_json_path.parent.mkdir(parents=True, exist_ok=True)
    deck_json_path.write_text(
        json.dumps(out_df.to_dict(orient="records"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[deck_spotify] updated deck csv -> {deck_path}")
    print(f"[deck_spotify] updated deck json -> {deck_json_path}")
    print(f"[deck_spotify] cache -> {cache_path}")


if __name__ == "__main__":
    main()

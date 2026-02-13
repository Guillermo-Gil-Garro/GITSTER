from __future__ import annotations

import argparse
import base64
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv


TRACK_ID_RE = re.compile(r"^[A-Za-z0-9]{22}$")
TRACK_URL_RE = re.compile(r"open\.spotify\.com/track/([^/?]+)")
TRACK_URI_RE = re.compile(r"spotify:track:([A-Za-z0-9]{22})")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


REPO_ROOT = repo_root()
PROCESSED_DIR = REPO_ROOT / "pipeline" / "data" / "processed"
CACHE_DIR = REPO_ROOT / "pipeline" / "cache"
REPORTS_DIR = REPO_ROOT / "pipeline" / "reports"


ENRICH_COLUMNS = [
    "spotify_track_id",
    "artists_all",
    "artists_count",
    "artists_ids",
    "artists_full_json",
    "album_id",
    "album_name",
    "album_release_date",
    "album_release_year",
    "duration_ms",
    "explicit",
    "popularity",
    "preview_url",
]


def clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def parse_release_year(date_value: str) -> str:
    date_s = clean_str(date_value)
    if len(date_s) >= 4 and date_s[:4].isdigit():
        return date_s[:4]
    return ""


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


def first_valid_track_id(values: List[Any]) -> Optional[str]:
    for raw in values:
        track_id = normalize_track_id(raw)
        if track_id:
            return track_id
    return None


def list_linked_files(expansion: str) -> List[Path]:
    pattern = PROCESSED_DIR / f"instances_linked_{expansion}_*.csv"
    files = sorted(pattern.parent.glob(pattern.name))
    if files:
        return files

    # Compatibilidad legacy.
    legacy = PROCESSED_DIR / f"instances_linked_{expansion}_Guille.csv"
    return [legacy] if legacy.exists() else []


def build_track_id_map_from_linked(expansion: str) -> Dict[str, str]:
    linked_files = list_linked_files(expansion)
    if not linked_files:
        return {}

    mapping: Dict[str, str] = {}
    for path in linked_files:
        df = pd.read_csv(path).fillna("")
        if "canonical_id" not in df.columns:
            continue

        for row in df.itertuples(index=False):
            canonical_id = clean_str(getattr(row, "canonical_id", ""))
            if not canonical_id or canonical_id in mapping:
                continue

            track_id = first_valid_track_id(
                [
                    getattr(row, "track_id", ""),
                    getattr(row, "spotify_uri", ""),
                    getattr(row, "spotify_url", ""),
                ]
            )
            if track_id:
                mapping[canonical_id] = track_id

    return mapping


def load_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    if isinstance(payload, dict) and isinstance(payload.get("tracks"), dict):
        tracks = payload["tracks"]
    elif isinstance(payload, dict):
        tracks = payload
    else:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for key, value in tracks.items():
        if isinstance(value, dict):
            out[str(key)] = value
    return out


def save_cache(path: Path, cache: Dict[str, Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 2,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "tracks": cache,
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


class SpotifyClient:
    def __init__(self, client_id: str, client_secret: str, timeout: float = 30.0):
        self.client_id = clean_str(client_id)
        self.client_secret = clean_str(client_secret)
        self.timeout = float(timeout)
        self.session = requests.Session()
        self._token: Optional[str] = None
        self._expires_at = 0.0

    def get_token(self, force_refresh: bool = False) -> str:
        now = time.time()
        if (not force_refresh) and self._token and now < (self._expires_at - 60):
            return self._token

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
            raise RuntimeError("Spotify token endpoint did not return access_token")

        self._token = token
        self._expires_at = time.time() + max(60, expires_in)
        return token

    def fetch_tracks_batch(
        self,
        track_ids: List[str],
        market: str,
        max_retry_after: int,
        max_attempts: int = 6,
    ) -> requests.Response:
        ids_param = ",".join(track_ids)
        refreshed_after_401 = False

        for attempt in range(1, max_attempts + 1):
            token = self.get_token(force_refresh=False)
            headers = {"Authorization": f"Bearer {token}"}
            resp = self.session.get(
                "https://api.spotify.com/v1/tracks",
                params={"ids": ids_param, "market": market},
                headers=headers,
                timeout=self.timeout,
            )

            if resp.status_code == 401 and not refreshed_after_401:
                self.get_token(force_refresh=True)
                refreshed_after_401 = True
                continue

            if resp.status_code == 429:
                retry_after_raw = clean_str(resp.headers.get("Retry-After", "1"))
                retry_after = int(retry_after_raw) if retry_after_raw.isdigit() else 1
                if retry_after > int(max_retry_after):
                    raise RuntimeError(
                        f"Spotify Retry-After {retry_after}s exceeds max_retry_after={max_retry_after}"
                    )
                if attempt == max_attempts:
                    return resp
                time.sleep(float(retry_after) + 0.2)
                continue

            if resp.status_code in {500, 502, 503, 504} and attempt < max_attempts:
                backoff = min(20.0, 0.5 * (2 ** (attempt - 1)))
                time.sleep(backoff + 0.2)
                continue

            return resp

        raise RuntimeError("Spotify /v1/tracks request exhausted retries")


def parse_track_payload(track: Dict[str, Any]) -> Dict[str, Any]:
    artists = track.get("artists") or []
    artist_names: List[str] = []
    artist_ids: List[str] = []
    full_rows: List[Dict[str, str]] = []

    for artist in artists:
        if not isinstance(artist, dict):
            continue
        name = clean_str(artist.get("name"))
        artist_id = clean_str(artist.get("id"))
        if name:
            artist_names.append(name)
        if artist_id:
            artist_ids.append(artist_id)
        if name or artist_id:
            full_rows.append({"id": artist_id, "name": name})

    album = track.get("album") or {}
    release_date = clean_str(album.get("release_date"))

    return {
        "spotify_track_id": clean_str(track.get("id")),
        "artists_all": ", ".join(artist_names),
        "artists_count": int(len(artist_names)),
        "artists_ids": ", ".join([a for a in artist_ids if a]),
        "artists_full_json": json.dumps(full_rows, ensure_ascii=False),
        "album_id": clean_str(album.get("id")),
        "album_name": clean_str(album.get("name")),
        "album_release_date": release_date,
        "album_release_year": parse_release_year(release_date),
        "duration_ms": track.get("duration_ms", ""),
        "explicit": track.get("explicit", ""),
        "popularity": track.get("popularity", ""),
        "preview_url": clean_str(track.get("preview_url")),
    }


def split_batches(items: List[str], batch_size: int) -> List[List[str]]:
    bsize = max(1, min(50, int(batch_size)))
    return [items[i : i + bsize] for i in range(0, len(items), bsize)]


def extract_row_track_ids(df: pd.DataFrame, linked_map: Dict[str, str]) -> List[Optional[str]]:
    out: List[Optional[str]] = []
    for row in df.itertuples(index=False):
        canonical_id = clean_str(getattr(row, "canonical_id", ""))
        row_track = first_valid_track_id(
            [
                getattr(row, "track_id", ""),
                getattr(row, "spotify_uri", ""),
                getattr(row, "spotify_url", ""),
            ]
        )
        if row_track:
            out.append(row_track)
            continue

        linked_track = linked_map.get(canonical_id, "")
        out.append(clean_str(linked_track) or None)
    return out


def enrich_rows(df: pd.DataFrame, row_track_ids: List[Optional[str]], cache: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    out = df.copy()
    defaults: Dict[str, Any] = {
        "spotify_track_id": "",
        "artists_all": "",
        "artists_count": 0,
        "artists_ids": "",
        "artists_full_json": "",
        "album_id": "",
        "album_name": "",
        "album_release_date": "",
        "album_release_year": "",
        "duration_ms": "",
        "explicit": "",
        "popularity": "",
        "preview_url": "",
    }

    rows: List[Dict[str, Any]] = []
    for track_id in row_track_ids:
        if not track_id:
            rows.append(dict(defaults))
            continue

        entry = cache.get(track_id, {})
        if not isinstance(entry, dict) or entry.get("_error"):
            row = dict(defaults)
            row["spotify_track_id"] = track_id
            rows.append(row)
            continue

        row = dict(defaults)
        for key in ENRICH_COLUMNS:
            if key in entry:
                row[key] = entry.get(key, defaults.get(key, ""))
        if (not clean_str(row.get("artists_all", ""))) and clean_str(entry.get("artists_full", "")):
            row["artists_all"] = clean_str(entry.get("artists_full", ""))
        if (not clean_str(row.get("artists_ids", ""))) and clean_str(entry.get("artists_ids", "")):
            row["artists_ids"] = clean_str(entry.get("artists_ids", ""))
        if not clean_str(row.get("spotify_track_id", "")):
            row["spotify_track_id"] = track_id
        try:
            row["artists_count"] = int(float(row.get("artists_count", 0) or 0))
        except Exception:
            row["artists_count"] = 0
        if row["artists_count"] <= 0 and clean_str(row.get("artists_all", "")):
            row["artists_count"] = len([p for p in row["artists_all"].split(",") if clean_str(p)])
        rows.append(row)

    enrich_df = pd.DataFrame(rows)
    for col in ENRICH_COLUMNS:
        if col not in enrich_df.columns:
            enrich_df[col] = defaults[col]
    for col in ENRICH_COLUMNS:
        out[col] = enrich_df[col]
    return out


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Enrich canonical songs with Spotify /v1/tracks batch metadata.")
    ap.add_argument("--expansion", default="I")
    ap.add_argument("--input", default=None, help="Default: pipeline/data/processed/canonical_songs_{EXP}.csv")
    ap.add_argument(
        "--output",
        default=None,
        help="Default: pipeline/data/processed/canonical_songs_{EXP}_spotify.csv",
    )
    ap.add_argument(
        "--cache",
        default=str(CACHE_DIR / "spotify_tracks_cache.json"),
        help="JSON cache path for Spotify tracks responses.",
    )
    ap.add_argument("--market", default="ES")
    ap.add_argument("--batch-size", type=int, default=50, help="Max 50 by Spotify API contract.")
    ap.add_argument("--sleep", type=float, default=0.25, help="Sleep between batch requests.")
    ap.add_argument("--progress-every", type=int, default=10)
    ap.add_argument("--limit", type=int, default=0, help="Process only first N unique track IDs (0=all).")
    ap.add_argument("--force-refresh", action="store_true", help="Ignore cache entries and refetch from API.")
    ap.add_argument("--max-retry-after", type=int, default=900)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    expansion = clean_str(args.expansion) or "I"

    in_path = Path(args.input) if args.input else (PROCESSED_DIR / f"canonical_songs_{expansion}.csv")
    out_path = Path(args.output) if args.output else (PROCESSED_DIR / f"canonical_songs_{expansion}_spotify.csv")
    cache_path = Path(args.cache)

    if not in_path.exists():
        raise FileNotFoundError(f"Input canonical songs not found: {in_path}")

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path).fillna("")
    if "canonical_id" not in df.columns:
        raise ValueError(f"{in_path} must include canonical_id")
    df["canonical_id"] = df["canonical_id"].astype(str).str.strip()

    linked_map = build_track_id_map_from_linked(expansion)
    row_track_ids = extract_row_track_ids(df, linked_map)

    unique_ids: List[str] = []
    seen: set[str] = set()
    for track_id in row_track_ids:
        if track_id and track_id not in seen:
            seen.add(track_id)
            unique_ids.append(track_id)

    if int(args.limit) > 0:
        unique_ids = unique_ids[: int(args.limit)]
        selected = set(unique_ids)
        row_track_ids = [tid if (tid in selected) else None for tid in row_track_ids]

    cache = load_cache(cache_path)

    to_fetch = [
        track_id
        for track_id in unique_ids
        if bool(args.force_refresh)
        or track_id not in cache
        or (isinstance(cache.get(track_id), dict) and cache.get(track_id, {}).get("_error"))
    ]

    if to_fetch:
        load_dotenv()
        client_id = clean_str(os.getenv("SPOTIFY_CLIENT_ID"))
        client_secret = clean_str(os.getenv("SPOTIFY_CLIENT_SECRET"))
        if not client_id or not client_secret:
            raise SystemExit("Missing SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET in environment/.env")

        client = SpotifyClient(client_id=client_id, client_secret=client_secret)
        batches = split_batches(to_fetch, int(args.batch_size))

        for i, batch_ids in enumerate(batches, start=1):
            try:
                resp = client.fetch_tracks_batch(
                    track_ids=batch_ids,
                    market=clean_str(args.market) or "ES",
                    max_retry_after=int(args.max_retry_after),
                )
            except Exception as exc:
                for track_id in batch_ids:
                    cache[track_id] = {
                        "_error": "request_exception",
                        "status_code": "",
                        "detail": clean_str(exc),
                    }
                save_cache(cache_path, cache)
                continue

            if resp.status_code == 200:
                payload = resp.json()
                tracks = payload.get("tracks") or []
                for track_id, track_obj in zip(batch_ids, tracks):
                    if isinstance(track_obj, dict):
                        cache[track_id] = parse_track_payload(track_obj)
                    else:
                        cache[track_id] = {
                            "_error": "not_found",
                            "status_code": 404,
                            "detail": "",
                        }
            else:
                status = int(resp.status_code)
                detail = clean_str(resp.text)[:1000]
                for track_id in batch_ids:
                    cache[track_id] = {
                        "_error": f"http_{status}",
                        "status_code": status,
                        "detail": detail,
                    }

            save_cache(cache_path, cache)
            if float(args.sleep) > 0:
                time.sleep(float(args.sleep))
            if int(args.progress_every) > 0 and (i % int(args.progress_every) == 0 or i == len(batches)):
                print(f"[spotify_tracks] batches {i}/{len(batches)}")

    enriched = enrich_rows(df=df, row_track_ids=row_track_ids, cache=cache)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(out_path, index=False, encoding="utf-8")

    missing_track_id = int(sum(1 for v in row_track_ids if not v))
    failed_unique = int(
        sum(1 for tid in unique_ids if isinstance(cache.get(tid), dict) and cache.get(tid, {}).get("_error"))
    )

    failed_rows = [
        {"track_id": tid, "reason": cache.get(tid, {}).get("_error", ""), "status_code": cache.get(tid, {}).get("status_code", "")}
        for tid in unique_ids
        if isinstance(cache.get(tid), dict) and cache.get(tid, {}).get("_error")
    ]
    failed_report_path = REPORTS_DIR / f"spotify_tracks_failed_{expansion}.csv"
    pd.DataFrame(failed_rows, columns=["track_id", "reason", "status_code"]).to_csv(
        failed_report_path,
        index=False,
        encoding="utf-8",
    )

    print(f"[spotify_tracks] input={in_path}")
    print(f"[spotify_tracks] output={out_path}")
    print(f"[spotify_tracks] cache={cache_path}")
    print(f"[spotify_tracks] linked_track_ids={len(linked_map)}")
    print(f"[spotify_tracks] total_rows={len(df)} unique_track_ids={len(unique_ids)}")
    print(f"[spotify_tracks] missing_track_id_rows={missing_track_id} failed_unique_track_ids={failed_unique}")
    print(f"[spotify_tracks] failed_report={failed_report_path}")


if __name__ == "__main__":
    main()

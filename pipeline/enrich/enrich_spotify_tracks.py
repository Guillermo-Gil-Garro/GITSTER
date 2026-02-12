import argparse
import base64
import json
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv


TRACK_ID_RE = re.compile(r"^[A-Za-z0-9]{22}$")
TRACK_URL_RE = re.compile(r"open\.spotify\.com/track/([^/?]+)")

ENRICH_COLUMNS = [
    "artists_full",
    "artists_ids",
    "album_id",
    "album_name",
    "album_release_date",
    "album_release_year",
    "duration_ms",
    "explicit",
    "popularity",
    "preview_url",
]


class RateLimitTooLongError(Exception):
    def __init__(self, track_id: str, retry_after: float):
        self.track_id = track_id
        self.retry_after = retry_after
        super().__init__(f"Retry-After too large for track_id={track_id}: {retry_after}")


class SpotifyClient:
    def __init__(self, client_id: str, client_secret: str, timeout: float = 30.0):
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.session = requests.Session()
        self.access_token: str | None = None
        self.expires_at: float = 0.0

    def get_token(self, force_refresh: bool = False) -> str:
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
        token = str(payload.get("access_token", "")).strip()
        expires_in = int(payload.get("expires_in", 3600))
        if not token:
            raise RuntimeError("No access_token received from Spotify token endpoint")

        self.access_token = token
        self.expires_at = time.time() + max(60, expires_in)
        return token

    def request_track(
        self,
        track_id: str,
        market: str,
        max_retry_after: int,
        max_attempts: int = 8,
    ) -> requests.Response:
        url = f"https://api.spotify.com/v1/tracks/{track_id}"
        refreshed_after_401 = False
        last_exc: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            token = self.get_token(force_refresh=False)
            headers = {"Authorization": f"Bearer {token}"}
            try:
                resp = self.session.get(
                    url,
                    params={"market": market},
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_exc = exc
                if attempt == max_attempts:
                    break
                sleep_s = min(30.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.35)
                print(
                    f"network_error track_id={track_id} attempt={attempt}/{max_attempts} "
                    f"sleep={sleep_s:.2f}s error={exc}"
                )
                time.sleep(sleep_s)
                continue

            if resp.status_code == 401 and (not refreshed_after_401):
                self.get_token(force_refresh=True)
                refreshed_after_401 = True
                if attempt < max_attempts:
                    continue

            if resp.status_code == 429:
                retry_after_raw = str(resp.headers.get("Retry-After", "")).strip()
                retry_after = float(retry_after_raw) if retry_after_raw.isdigit() else 1.0
                print(
                    f"429 status=429 track_id={track_id} Retry-After={retry_after_raw or 'missing->1'}"
                )

                if retry_after > float(max_retry_after):
                    raise RateLimitTooLongError(track_id=track_id, retry_after=retry_after)

                if attempt == max_attempts:
                    return resp

                sleep_s = retry_after + random.uniform(0.0, 0.35)
                print(
                    f"rate_limit_wait track_id={track_id} attempt={attempt}/{max_attempts} "
                    f"sleep={sleep_s:.2f}s"
                )
                time.sleep(sleep_s)
                continue

            if resp.status_code in (500, 502, 503, 504):
                if attempt == max_attempts:
                    return resp
                sleep_s = min(30.0, 0.5 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.35)
                print(
                    f"server_error status={resp.status_code} track_id={track_id} "
                    f"attempt={attempt}/{max_attempts} sleep={sleep_s:.2f}s"
                )
                time.sleep(sleep_s)
                continue

            return resp

        if last_exc is not None:
            raise RuntimeError(f"HTTP request failed after retries: {last_exc}") from last_exc
        raise RuntimeError("HTTP request failed after retries")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


REPO_ROOT = repo_root()
PROCESSED_DIR = REPO_ROOT / "pipeline" / "data" / "processed"
CACHE_PATH = REPO_ROOT / "pipeline" / "cache" / "spotify_tracks_cache.json"
REPORTS_DIR = REPO_ROOT / "pipeline" / "reports"


def clean_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def normalize_track_id(raw: Any) -> str | None:
    value = clean_str(raw)
    if not value:
        return None

    if TRACK_ID_RE.fullmatch(value):
        return value

    if value.startswith("spotify:track:"):
        candidate = value.split("spotify:track:", 1)[1].strip()
        candidate = candidate.split("?")[0].split("/")[0]
        return candidate if TRACK_ID_RE.fullmatch(candidate) else None

    m = TRACK_URL_RE.search(value)
    if m:
        candidate = m.group(1).split("?")[0].strip()
        return candidate if TRACK_ID_RE.fullmatch(candidate) else None

    return None


def extract_row_track_id(row: pd.Series) -> tuple[str | None, str, str]:
    ordered = [
        ("track_id", row.get("track_id", "")),
        ("spotify_uri", row.get("spotify_uri", "")),
        ("spotify_url", row.get("spotify_url", "")),
    ]

    first_invalid_value = ""
    first_invalid_reason = ""

    for source, raw in ordered:
        raw_str = clean_str(raw)
        if not raw_str:
            continue

        normalized = normalize_track_id(raw_str)
        if normalized:
            return normalized, raw_str, ""

        if not first_invalid_value:
            first_invalid_value = raw_str
            first_invalid_reason = f"invalid_{source}_format"

    if first_invalid_value:
        return None, first_invalid_value, first_invalid_reason

    return None, "", "missing_track_id_spotify_uri_spotify_url"


def parse_release_year(release_date: str) -> int | str:
    date_str = clean_str(release_date)
    if len(date_str) >= 4 and date_str[:4].isdigit():
        return int(date_str[:4])
    return ""


def parse_track_payload(track: dict[str, Any]) -> dict[str, Any]:
    artists = track.get("artists") or []
    artists_full = ", ".join(
        clean_str(a.get("name"))
        for a in artists
        if isinstance(a, dict) and clean_str(a.get("name"))
    )
    artists_ids = ",".join(
        clean_str(a.get("id"))
        for a in artists
        if isinstance(a, dict) and clean_str(a.get("id"))
    )

    album = track.get("album") or {}
    release_date = clean_str(album.get("release_date"))

    return {
        "artists_full": artists_full,
        "artists_ids": artists_ids,
        "album_id": clean_str(album.get("id")),
        "album_name": clean_str(album.get("name")),
        "album_release_date": release_date,
        "album_release_year": parse_release_year(release_date),
        "duration_ms": track.get("duration_ms", ""),
        "explicit": track.get("explicit", ""),
        "popularity": track.get("popularity", ""),
        "preview_url": clean_str(track.get("preview_url")),
    }


def load_cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}

    if isinstance(raw, dict) and isinstance(raw.get("tracks"), dict):
        return {k: v for k, v in raw["tracks"].items() if isinstance(v, dict)}

    if isinstance(raw, dict):
        return {k: v for k, v in raw.items() if isinstance(v, dict)}

    return {}


def save_cache(path: Path, cache: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "tracks": cache,
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def build_input_path(owner: str) -> Path:
    return PROCESSED_DIR / f"spotify_liked_songs_from_export_{owner}.csv"


def build_output_path(owner: str) -> Path:
    return PROCESSED_DIR / f"spotify_liked_songs_from_export_{owner}_enriched.csv"


def build_invalid_report_path(owner: str) -> Path:
    return REPORTS_DIR / f"enrich_invalid_track_ids_{owner}.csv"


def build_failed_report_path(owner: str) -> Path:
    return REPORTS_DIR / f"enrich_failed_{owner}.csv"


def merge_enrichment(df: pd.DataFrame, row_track_ids: list[str | None], data_map: dict[str, dict[str, Any]]) -> pd.DataFrame:
    out = df.copy()
    for col in ENRICH_COLUMNS:
        out[col] = [
            data_map.get(track_id, {}).get(col, "") if track_id else ""
            for track_id in row_track_ids
        ]
    return out


def write_partial_enriched(
    df: pd.DataFrame,
    row_track_ids: list[str | None],
    enriched_map: dict[str, dict[str, Any]],
    out_path: Path,
) -> None:
    partial_df = merge_enrichment(df, row_track_ids, enriched_map)
    partial_df.to_csv(out_path, index=False, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Enrich Spotify export tracks using /v1/tracks/{id}.")
    ap.add_argument("--owner", required=True, help="Owner label used in input/output filenames")
    ap.add_argument("--market", default="ES", help="Spotify market (default: ES)")
    ap.add_argument("--sleep", type=float, default=0.6, help="Sleep between calls in seconds (default: 0.6)")
    ap.add_argument("--limit", type=int, default=None, help="Optional max number of unique track IDs to process")
    ap.add_argument("--force-refresh", action="store_true", help="Ignore cache and fetch all IDs again")
    ap.add_argument("--progress-every", type=int, default=50, help="Progress print frequency (default: 50)")
    ap.add_argument(
        "--max-retry-after",
        type=int,
        default=900,
        help="Abort if Spotify Retry-After is greater than this number of seconds (default: 900)",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    load_dotenv()
    client_id = clean_str(os.getenv("SPOTIFY_CLIENT_ID"))
    client_secret = clean_str(os.getenv("SPOTIFY_CLIENT_SECRET"))
    if not client_id or not client_secret:
        raise SystemExit("Missing SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET in .env")

    in_path = build_input_path(args.owner)
    if not in_path.exists():
        raise SystemExit(f"Input CSV not found: {in_path}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path).fillna("")

    row_track_ids: list[str | None] = []
    invalid_rows: list[dict[str, Any]] = []

    for idx, row in df.iterrows():
        track_id, raw_value, reason = extract_row_track_id(row)
        row_track_ids.append(track_id)
        if track_id is None:
            invalid_rows.append(
                {
                    "row_index": int(idx),
                    "raw_value": raw_value,
                    "reason": reason,
                }
            )

    ordered_unique_ids: list[str] = []
    seen: set[str] = set()
    for tid in row_track_ids:
        if tid and tid not in seen:
            seen.add(tid)
            ordered_unique_ids.append(tid)

    if args.limit is not None and args.limit >= 0:
        ordered_unique_ids = ordered_unique_ids[: args.limit]
        selected = set(ordered_unique_ids)
        row_track_ids = [tid if (tid in selected) else None for tid in row_track_ids]

    cache = load_cache(CACHE_PATH)
    client = SpotifyClient(client_id=client_id, client_secret=client_secret)

    enriched_map: dict[str, dict[str, Any]] = {}
    failed_rows: list[dict[str, Any]] = []

    from_cache = 0
    fetched_ok = 0
    processed = 0
    aborted = False
    abort_message = ""

    out_path = build_output_path(args.owner)

    for track_id in ordered_unique_ids:
        processed += 1

        if (not args.force_refresh) and track_id in cache:
            entry = cache.get(track_id, {})
            if isinstance(entry, dict) and entry.get("_error"):
                failed_rows.append(
                    {
                        "track_id": track_id,
                        "reason": clean_str(entry.get("_error")),
                        "status_code": entry.get("status_code", ""),
                        "detail": clean_str(entry.get("detail", "")),
                    }
                )
            else:
                enriched_map[track_id] = entry
            from_cache += 1
        else:
            try:
                resp = client.request_track(
                    track_id=track_id,
                    market=args.market,
                    max_retry_after=args.max_retry_after,
                )
            except RateLimitTooLongError as exc:
                reason = "rate_limited_too_long"
                detail = f"Retry-After={int(exc.retry_after)}"
                failed_rows.append(
                    {
                        "track_id": track_id,
                        "reason": reason,
                        "status_code": 429,
                        "detail": detail,
                    }
                )
                cache[track_id] = {"_error": reason, "status_code": 429, "detail": detail}

                save_cache(CACHE_PATH, cache)
                write_partial_enriched(df, row_track_ids, enriched_map, out_path)

                aborted = True
                abort_message = (
                    f"rate-limited; reintentar más tarde "
                    f"(track_id={track_id}, Retry-After={int(exc.retry_after)}s > max={args.max_retry_after}s)"
                )
                print(abort_message)
                break
            except Exception as exc:
                detail = f"request_exception: {exc}"
                failed_rows.append(
                    {
                        "track_id": track_id,
                        "reason": "request_exception",
                        "status_code": "",
                        "detail": detail,
                    }
                )
                cache[track_id] = {"_error": "request_exception", "status_code": "", "detail": detail}
                if args.sleep > 0:
                    time.sleep(args.sleep)
                if processed % 10 == 0:
                    save_cache(CACHE_PATH, cache)
                if processed % 50 == 0:
                    write_partial_enriched(df, row_track_ids, enriched_map, out_path)
                if args.progress_every > 0 and (processed % args.progress_every == 0):
                    print(f"processed {processed}/{len(ordered_unique_ids)}")
                continue

            if resp.status_code == 200:
                payload = resp.json()
                parsed = parse_track_payload(payload)
                enriched_map[track_id] = parsed
                cache[track_id] = parsed
                fetched_ok += 1
            elif resp.status_code == 404:
                cache[track_id] = {
                    "_error": "not_found",
                    "status_code": 404,
                    "detail": "",
                }
                failed_rows.append(
                    {
                        "track_id": track_id,
                        "reason": "not_found",
                        "status_code": 404,
                        "detail": "",
                    }
                )
            elif resp.status_code == 403:
                body = clean_str(resp.text)[:1000]
                cache[track_id] = {
                    "_error": "forbidden_single",
                    "status_code": 403,
                    "detail": body,
                }
                failed_rows.append(
                    {
                        "track_id": track_id,
                        "reason": "forbidden_single",
                        "status_code": 403,
                        "detail": body,
                    }
                )
            else:
                body = clean_str(resp.text)[:1000]
                reason = f"http_{resp.status_code}"
                cache[track_id] = {
                    "_error": reason,
                    "status_code": resp.status_code,
                    "detail": body,
                }
                failed_rows.append(
                    {
                        "track_id": track_id,
                        "reason": reason,
                        "status_code": resp.status_code,
                        "detail": body,
                    }
                )

            if args.sleep > 0:
                time.sleep(args.sleep)

        if processed % 10 == 0:
            save_cache(CACHE_PATH, cache)
        if processed % 50 == 0:
            write_partial_enriched(df, row_track_ids, enriched_map, out_path)

        if args.progress_every > 0 and (processed % args.progress_every == 0):
            print(f"processed {processed}/{len(ordered_unique_ids)}")

    save_cache(CACHE_PATH, cache)
    write_partial_enriched(df, row_track_ids, enriched_map, out_path)

    invalid_path = build_invalid_report_path(args.owner)
    pd.DataFrame(invalid_rows, columns=["row_index", "raw_value", "reason"]).to_csv(
        invalid_path,
        index=False,
        encoding="utf-8",
    )

    failed_path = build_failed_report_path(args.owner)
    pd.DataFrame(failed_rows, columns=["track_id", "reason", "status_code", "detail"]).to_csv(
        failed_path,
        index=False,
        encoding="utf-8",
    )

    print("QC summary")
    print(f"total_rows={len(df)}")
    print(f"unique_ids={len(ordered_unique_ids)}")
    print(f"from_cache={from_cache}")
    print(f"fetched_ok={fetched_ok}")
    print(f"invalid_ids={len(invalid_rows)}")
    print(f"failed={len(failed_rows)}")
    print(f"out_csv={out_path}")
    print(f"invalid_report={invalid_path}")
    print(f"failed_report={failed_path}")

    if aborted:
        raise SystemExit(f"rate-limited; reintentar más tarde. {abort_message}")


if __name__ == "__main__":
    main()
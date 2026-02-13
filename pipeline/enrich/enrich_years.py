import argparse
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, List

import pandas as pd
import requests
from rapidfuzz import fuzz


PROCESSED_DIR = Path("pipeline/data/processed")
REPORTS_DIR = Path("pipeline/reports")
MANUAL_DIR = Path("pipeline/manual")
CACHE_DIR = Path("pipeline/cache")

# Recording search endpoint (sin slash final para evitar redirects)
MB_ENDPOINT = "https://musicbrainz.org/ws/2/recording"
ITUNES_ENDPOINT = "https://itunes.apple.com/search"


def load_json(path: Path) -> Dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_dirs() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def normalize_str(s: str) -> str:
    return str(s or "").strip()


def pick_earliest_year_from_mb_recording(rec: Dict[str, Any]) -> Tuple[str, str]:
    """
    Devuelve (year, note) usando la fecha más antigua disponible en releases.
    year = "" si no hay fechas.
    """
    rels = rec.get("releases") or []
    years: List[int] = []
    for r in rels:
        date = normalize_str(r.get("date"))
        if len(date) >= 4 and date[:4].isdigit():
            years.append(int(date[:4]))
    if not years:
        return "", "no_release_dates"
    return str(min(years)), "earliest_release_year"


def mb_search_year(
    session: requests.Session,
    artists: str,
    title: str,
    user_agent: str,
    mb_cache: Dict[str, Any],
    cache_key: str,
    sleep_s: float,
) -> Dict[str, Any]:
    """
    MusicBrainz: busca una recording por (artist, title) y devuelve dict:
    {year, source, confidence, note}
    Cachea por cache_key (idealmente canonical_id).
    """
    if cache_key in mb_cache:
        return mb_cache[cache_key]

    headers = {"User-Agent": user_agent}
    params = {
        "query": f'recording:"{title}" AND artist:"{artists}"',
        "fmt": "json",
        "limit": 5,
    }

    # Rate limit conservador (solo si realmente llamamos a la API)
    if sleep_s > 0:
        time.sleep(sleep_s)

    try:
        resp = session.get(MB_ENDPOINT, params=params, headers=headers, timeout=30)
    except Exception as e:
        result = {
            "year": "",
            "source": "musicbrainz",
            "confidence": 0.0,
            "note": f"exception:{type(e).__name__}",
        }
        mb_cache[cache_key] = result
        return result

    if resp.status_code != 200:
        result = {
            "year": "",
            "source": "musicbrainz",
            "confidence": 0.0,
            "note": f"http_{resp.status_code}",
        }
        mb_cache[cache_key] = result
        return result

    data = resp.json()
    recs = data.get("recordings") or []
    if not recs:
        result = {
            "year": "",
            "source": "musicbrainz",
            "confidence": 0.0,
            "note": "no_matches",
        }
        mb_cache[cache_key] = result
        return result

    candidates = []
    for rec in recs:
        score = float(rec.get("score") or 0.0)  # 0..100
        rec_title = normalize_str(rec.get("title"))

        credit = rec.get("artist-credit") or []
        rec_artists = " ".join(
            [(a.get("name") or "") for a in credit if isinstance(a, dict)]
        ).strip()

        title_sim = (
            (fuzz.token_set_ratio(title, rec_title) / 100.0) if rec_title else 0.0
        )
        artist_sim = (
            (fuzz.token_set_ratio(artists, rec_artists) / 100.0) if rec_artists else 0.0
        )

        year, year_note = pick_earliest_year_from_mb_recording(rec)
        year_ok = 1.0 if year else 0.0

        # Heurística simple y estable: MB score + similitudes + pequeño bonus si hay año
        combined = (
            (score / 100.0) * 0.50
            + title_sim * 0.25
            + artist_sim * 0.20
            + year_ok * 0.05
        )
        candidates.append((combined, year, year_note, rec_title, rec_artists, score))

    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0]
    year = best[1]
    conf = round(float(best[0]), 3)

    result = {
        "year": year,
        "source": "musicbrainz",
        "confidence": conf,
        "note": best[2] if year else f"best_no_year:{best[2]}",
    }
    mb_cache[cache_key] = result
    return result


def itunes_search_year(
    session: requests.Session,
    artists: str,
    title: str,
    it_cache: Dict[str, Any],
    cache_key: str,
) -> Dict[str, Any]:
    """
    iTunes Search API fallback. Devuelve dict:
    {year, source, confidence, note}
    Cachea por cache_key (idealmente canonical_id).
    """
    if cache_key in it_cache:
        return it_cache[cache_key]

    params = {"term": f"{artists} {title}", "entity": "song", "limit": 5}
    try:
        resp = session.get(ITUNES_ENDPOINT, params=params, timeout=30)
    except Exception as e:
        result = {
            "year": "",
            "source": "itunes",
            "confidence": 0.0,
            "note": f"exception:{type(e).__name__}",
        }
        it_cache[cache_key] = result
        return result

    if resp.status_code != 200:
        result = {
            "year": "",
            "source": "itunes",
            "confidence": 0.0,
            "note": f"http_{resp.status_code}",
        }
        it_cache[cache_key] = result
        return result

    data = resp.json()
    results = data.get("results") or []
    if not results:
        result = {
            "year": "",
            "source": "itunes",
            "confidence": 0.0,
            "note": "no_matches",
        }
        it_cache[cache_key] = result
        return result

    r0 = results[0]
    date = normalize_str(r0.get("releaseDate"))
    year = date[:4] if len(date) >= 4 and date[:4].isdigit() else ""

    result = {
        "year": year,
        "source": "itunes",
        "confidence": 0.55 if year else 0.0,  # fallback: confianza media-baja
        "note": "top_result_releaseDate" if year else "no_releaseDate",
    }
    it_cache[cache_key] = result
    return result


def load_year_overrides() -> pd.DataFrame:
    path = MANUAL_DIR / "manual_year_overrides.csv"
    if not path.exists():
        return pd.DataFrame(columns=["canonical_id", "year", "note"])
    df = pd.read_csv(path).fillna("")
    df["canonical_id"] = df["canonical_id"].astype(str).str.strip()
    df["year"] = df["year"].astype(str).str.strip()
    return df


def write_partial(
    base_df: pd.DataFrame,
    years: List[str],
    sources: List[str],
    confs: List[float],
    notes: List[str],
    out_path: Path,
) -> None:
    tmp = base_df.copy()
    # Rellenar hasta longitud total (por si hacemos checkpoint a mitad)
    n = len(tmp)
    tmp["year"] = years + [""] * (n - len(years))
    tmp["year_source"] = sources + [""] * (n - len(sources))
    tmp["year_confidence"] = confs + [0.0] * (n - len(confs))
    tmp["year_note"] = notes + [""] * (n - len(notes))
    tmp.to_csv(out_path, index=False, encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expansion", default="I")
    ap.add_argument("--input", default=None)
    ap.add_argument("--output", default=None)
    ap.add_argument("--mode", choices=["mb", "itunes", "hybrid"], default="hybrid")
    ap.add_argument("--min-confidence", type=float, default=0.80)
    ap.add_argument("--user-agent", default="GITSTER/1.0 (local)")
    ap.add_argument("--mb-sleep", type=float, default=1.0)
    ap.add_argument(
        "--max-items", type=int, default=0, help="Procesa solo N canciones (0 = todas)"
    )
    ap.add_argument(
        "--print-every", type=int, default=25, help="Imprime progreso cada N canciones"
    )
    ap.add_argument(
        "--checkpoint-every",
        type=int,
        default=50,
        help="Guarda cachés + CSV parcial cada N canciones",
    )
    args = ap.parse_args()

    ensure_dirs()

    expansion = args.expansion
    if args.input:
        in_path = Path(args.input)
    else:
        spotify_first = PROCESSED_DIR / f"canonical_songs_{expansion}_spotify.csv"
        canonical_base = PROCESSED_DIR / f"canonical_songs_{expansion}.csv"
        in_path = spotify_first if spotify_first.exists() else canonical_base
    out_path = (
        Path(args.output)
        if args.output
        else (PROCESSED_DIR / f"canonical_songs_{expansion}_enriched.csv")
    )

    if not in_path.exists():
        raise FileNotFoundError(f"No existe: {in_path}")

    mb_cache_path = CACHE_DIR / "musicbrainz_cache.json"
    it_cache_path = CACHE_DIR / "itunes_cache.json"
    mb_cache = load_json(mb_cache_path)
    it_cache = load_json(it_cache_path)

    overrides_path = MANUAL_DIR / "manual_year_overrides.csv"
    if not overrides_path.exists():
        pd.DataFrame(columns=["canonical_id", "year", "note"]).to_csv(
            overrides_path, index=False, encoding="utf-8"
        )

    overrides_df = load_year_overrides()
    override_map = dict(zip(overrides_df["canonical_id"], overrides_df["year"]))

    df = pd.read_csv(in_path).fillna("")
    # Esperado: canonical_id, title_canon, artists_canon, year (vacío), etc.
    df["canonical_id"] = df["canonical_id"].astype(str).str.strip()
    df["title_canon"] = df["title_canon"].astype(str)
    df["artists_canon"] = df["artists_canon"].astype(str)

    total = len(df)
    if args.max_items and args.max_items > 0:
        total_to_process = min(args.max_items, total)
    else:
        total_to_process = total

    years: List[str] = []
    sources: List[str] = []
    confs: List[float] = []
    notes: List[str] = []

    unresolved_rows: List[Dict[str, Any]] = []

    session = requests.Session()

    start_ts = time.time()
    partial_path = PROCESSED_DIR / f"canonical_songs_{expansion}_enriched__partial.csv"
    unresolved_path = REPORTS_DIR / f"year_unresolved_{expansion}.csv"

    def checkpoint(i_done: int) -> None:
        save_json(mb_cache_path, mb_cache)
        save_json(it_cache_path, it_cache)
        write_partial(df, years, sources, confs, notes, partial_path)
        pd.DataFrame(unresolved_rows).to_csv(
            unresolved_path, index=False, encoding="utf-8"
        )
        print(
            f"Checkpoint -> caches + {partial_path.name} + {unresolved_path.name} (done={i_done})"
        )

    try:
        for i, row in enumerate(df.itertuples(index=False), start=1):
            if args.max_items and args.max_items > 0 and i > args.max_items:
                break

            cid = normalize_str(getattr(row, "canonical_id", ""))
            title = normalize_str(getattr(row, "title_canon", ""))
            artists = normalize_str(getattr(row, "artists_canon", ""))

            # 1) Override manual manda
            if cid and override_map.get(cid, ""):
                result = {
                    "year": override_map[cid],
                    "source": "manual",
                    "confidence": 1.0,
                    "note": "manual_override",
                }
            else:
                result = {"year": "", "source": "", "confidence": 0.0, "note": ""}

                # 2) MusicBrainz (si aplica)
                if args.mode in ("mb", "hybrid"):
                    result = mb_search_year(
                        session=session,
                        artists=artists,
                        title=title,
                        user_agent=args.user_agent,
                        mb_cache=mb_cache,
                        cache_key=cid or f"{artists} | {title}",
                        sleep_s=args.mb_sleep,
                    )

                # 3) Fallback iTunes (si híbrido y no hay año o baja confianza)
                if args.mode == "itunes" or (
                    args.mode == "hybrid"
                    and (
                        not result["year"]
                        or float(result["confidence"]) < float(args.min_confidence)
                    )
                ):
                    it = itunes_search_year(
                        session=session,
                        artists=artists,
                        title=title,
                        it_cache=it_cache,
                        cache_key=cid or f"{artists} | {title}",
                    )
                    # Tomamos iTunes si aporta año y mejora o MB no tenía año
                    if it["year"] and (
                        not result["year"]
                        or float(it["confidence"]) > float(result["confidence"])
                    ):
                        result = it

            years.append(normalize_str(result.get("year", "")))
            sources.append(normalize_str(result.get("source", "")))
            confs.append(float(result.get("confidence", 0.0) or 0.0))
            notes.append(normalize_str(result.get("note", "")))

            if not years[-1]:
                unresolved_rows.append(
                    {
                        "canonical_id": cid,
                        "title_canon": title,
                        "artists_canon": artists,
                        "note": notes[-1] or "unresolved",
                    }
                )

            # Progreso
            if args.print_every > 0 and i % args.print_every == 0:
                elapsed = time.time() - start_ts
                rate = i / elapsed if elapsed > 0 else 0.0
                remaining = (total_to_process - i) / rate if rate > 0 else float("inf")
                eta_min = int(remaining // 60) if remaining != float("inf") else -1
                print(
                    f"[{i}/{total_to_process}] year={years[-1]} src={sources[-1]} conf={confs[-1]:.2f} "
                    f"ETA~{eta_min}m :: {artists} - {title}"
                )

            # Checkpoint
            if args.checkpoint_every > 0 and i % args.checkpoint_every == 0:
                checkpoint(i_done=i)

        # Final write
        df["year"] = years + [""] * (len(df) - len(years))
        df["year_source"] = sources + [""] * (len(df) - len(sources))
        df["year_confidence"] = confs + [0.0] * (len(df) - len(confs))
        df["year_note"] = notes + [""] * (len(df) - len(notes))

        df.to_csv(out_path, index=False, encoding="utf-8")
        pd.DataFrame(unresolved_rows).to_csv(
            unresolved_path, index=False, encoding="utf-8"
        )

        save_json(mb_cache_path, mb_cache)
        save_json(it_cache_path, it_cache)

        print(f"OK enriched -> {out_path}")
        print(f"OK unresolved -> {unresolved_path} ({len(unresolved_rows)} unresolved)")
        print(f"OK caches -> {mb_cache_path.name}, {it_cache_path.name}")
        print(f"OK overrides -> {overrides_path}")

    except KeyboardInterrupt:
        print("\nKeyboardInterrupt: guardando checkpoint antes de salir...")
        checkpoint(i_done=len(years))
        raise


if __name__ == "__main__":
    main()

import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd


PROCESSED_DIR = Path("pipeline/data/processed")
REPORTS_DIR = Path("pipeline/reports")
MANUAL_DIR = Path("pipeline/manual")
RAW_DIR = Path("pipeline/data/raw/spotify_export")


def find_input_csv(owner: str) -> Path:
    """
    Compatibilidad:
    - Nuevo: spotify_liked_songs_from_export_<OWNER>.csv
    - Legacy: spotify_liked_songs_from_export__<OWNER>.csv
    - Fallback: spotify_liked_songs_from_export.csv
    """
    candidates = [
        PROCESSED_DIR / f"spotify_liked_songs_from_export_{owner}.csv",
        PROCESSED_DIR / f"spotify_liked_songs_from_export__{owner}.csv",
        PROCESSED_DIR / "spotify_liked_songs_from_export.csv",
    ]
    for p in candidates:
        if p.exists():
            return p

    raise FileNotFoundError(
        f"No encuentro CSV de entrada en {PROCESSED_DIR}. "
        f"Esperaba alguno de: {[c.name for c in candidates]}"
    )


def list_owners_from_raw() -> list[str]:
    if not RAW_DIR.exists():
        return []
    owners = []
    for p in RAW_DIR.iterdir():
        if p.is_dir() and (p / "YourLibrary.json").exists():
            owners.append(p.name)
    return sorted(owners, key=lambda s: s.lower())


def build_for_owner(owner: str, expansion: str, processed_month: str) -> tuple[Path, Path]:
    in_path = find_input_csv(owner)
    df = pd.read_csv(in_path).fillna("")

    # Esperamos columnas del parser: owner_label,title,artists,spotify_uri,track_id,spotify_url,year
    # (si el parser no trae owner_label, lo forzamos)
    if "owner_label" not in df.columns:
        df["owner_label"] = owner

    # Construcción de instances estándar
    out = pd.DataFrame(
        {
            "owner_label": owner,
            "platform": "spotify",
            "source_type": "liked_songs_export",
            "source_context": "YourLibrary.json",
            "processed_month": processed_month,
            "expansion_code": expansion,
            "title_raw": df.get("title", ""),
            "artists_raw": df.get("artists", ""),
            "title_trim": df.get("title", "").astype(str).str.strip(),
            "artists_trim": df.get("artists", "").astype(str).str.strip(),
            "spotify_uri": df.get("spotify_uri", ""),
            "track_id": df.get("track_id", ""),
            "spotify_url": df.get("spotify_url", ""),
            "year": df.get("year", ""),
            # NUEVO: álbum (si existe en parser)
            "album_name_raw": df.get("album_name", ""),
            "album_name_trim": df.get("album_name", "").astype(str).str.strip(),
            "album_uri": df.get("album_uri", ""),
            "album_id": df.get("album_id", ""),
            "album_release_date": df.get("album_release_date", ""),
        }
    )

    out["canonical_key"] = out["artists_trim"] + " | " + out["title_trim"]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)

    out_path = PROCESSED_DIR / f"instances_{expansion}_{owner}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8")

    # Mini QC report (CSV sencillo)
    qc = {
        "rows": [len(out)],
        "unique_canonical_key": [out["canonical_key"].nunique()],
        "missing_spotify_url": [(out["spotify_url"].astype(str).str.strip() == "").sum()],
        "missing_track_id": [(out["track_id"].astype(str).str.strip() == "").sum()],
        "missing_year": [(out["year"].astype(str).str.strip() == "").sum()],
        "missing_album_id": [(out["album_id"].astype(str).str.strip() == "").sum()],
        "missing_album_name": [(out["album_name_trim"].astype(str).str.strip() == "").sum()],
    }
    qc_path = REPORTS_DIR / f"qc_instances_{expansion}_{owner}.csv"
    pd.DataFrame(qc).to_csv(qc_path, index=False, encoding="utf-8")

    # Stub de merges manuales (para el futuro)
    merges_path = MANUAL_DIR / "manual_merges.csv"
    if not merges_path.exists():
        pd.DataFrame(
            columns=[
                "alias_title_trim",
                "alias_artists_trim",
                "canonical_id_target",
                "note",
            ]
        ).to_csv(merges_path, index=False, encoding="utf-8")

    print(f"OK instances -> {out_path}")
    print(f"OK qc -> {qc_path}")
    return out_path, qc_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--owner", default=None, help="Owner concreto (ej. Guille). Si usas --all, se ignora.")
    ap.add_argument("--all", action="store_true", help="Genera instances para todos los owners detectados en raw/")
    ap.add_argument("--expansion", default="I")
    ap.add_argument("--processed-month", default=None, help="YYYY-MM (si no, usa mes actual)")
    args = ap.parse_args()

    expansion = args.expansion
    processed_month = args.processed_month or datetime.now().strftime("%Y-%m")

    if args.all:
        owners = list_owners_from_raw()
        if not owners:
            raise SystemExit(f"No detecto owners en {RAW_DIR}. Esperaba raw/spotify_export/<OWNER>/YourLibrary.json")
        for o in owners:
            build_for_owner(o, expansion=expansion, processed_month=processed_month)
        return

    owner = args.owner or "Guille"
    build_for_owner(owner, expansion=expansion, processed_month=processed_month)


if __name__ == "__main__":
    main()

import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd


PROCESSED_DIR = Path("pipeline/data/processed")
REPORTS_DIR = Path("pipeline/reports")
MANUAL_DIR = Path("pipeline/manual")


def find_input_csv(owner: str) -> Path:
    # Preferimos el output por owner, si existe
    preferred = PROCESSED_DIR / f"spotify_liked_songs_from_export__{owner}.csv"
    if preferred.exists():
        return preferred

    # Fallback al genérico (por si se generó antes)
    generic = PROCESSED_DIR / "spotify_liked_songs_from_export.csv"
    if generic.exists():
        return generic

    raise FileNotFoundError(
        f"No encuentro CSV de entrada en {PROCESSED_DIR}. "
        f"Esperaba {preferred.name} o {generic.name}."
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--owner", default="Guille")
    ap.add_argument("--expansion", default="I")
    ap.add_argument(
        "--processed-month", default=None, help="YYYY-MM (si no, usa mes actual)"
    )
    args = ap.parse_args()

    owner = args.owner
    expansion = args.expansion
    processed_month = args.processed_month or datetime.now().strftime("%Y-%m")

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
            # year queda vacío por ahora (fuentes externas después)
            "year": df.get("year", ""),
        }
    )

    out["canonical_key"] = out["artists_trim"] + " | " + out["title_trim"]

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)

    out_path = PROCESSED_DIR / f"instances_{expansion}__{owner}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8")

    # Mini QC report (CSV sencillo)
    qc = {
        "rows": [len(out)],
        "unique_canonical_key": [out["canonical_key"].nunique()],
        "missing_spotify_url": [(out["spotify_url"] == "").sum()],
        "missing_track_id": [(out["track_id"] == "").sum()],
        "missing_year": [(out["year"] == "").sum()],
    }
    qc_path = REPORTS_DIR / f"qc_instances_{expansion}__{owner}.csv"
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
    print(f"OK manual merges -> {merges_path}")


if __name__ == "__main__":
    main()

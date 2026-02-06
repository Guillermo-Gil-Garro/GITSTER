import argparse
import json
import re
from pathlib import Path

import pandas as pd


PROCESSED_DIR = Path("pipeline/data/processed")
REPORTS_DIR = Path("pipeline/reports")


# Quita solo “feat/ft/featuring” en paréntesis o corchetes.
# Mantiene otros paréntesis (ej. (Call Me By Your Name), (Da Ba Dee)).
FEAT_PAREN_RE = re.compile(
    r"[\(\[]\s*(feat\.?|ft\.?|featuring)\b.*?[\)\]]", flags=re.IGNORECASE
)

# Keywords típicos de “versiones” que NO quieres en la carta
VERSION_KEYWORDS = [
    "remaster",
    "radio edit",
    "edit",
    "re-edit",
    "mix",
    "remix",
    "version",
    "live",
    "acoustic",
    "demo",
    "karaoke",
    "instrumental",
    "sped up",
    "slowed",
    "extended",
    "club",
    "mono",
    "stereo",
]


def clean_title_display(title: str) -> str:
    s = str(title or "").strip()

    # 1) quitar (feat. X) / [feat. X]
    s = FEAT_PAREN_RE.sub("", s).strip()

    # 2) manejar sufijos tipo " - 2011 Remastered" / " - Radio Edit"
    parts = [p.strip() for p in s.split(" - ")]
    if len(parts) >= 2:
        suffix = " ".join(parts[1:]).lower()
        if any(k in suffix for k in VERSION_KEYWORDS):
            s = parts[0].strip()

    # 3) quitar paréntesis de “remaster” si vinieran como "(2011 Remastered)"
    s = re.sub(r"\(([^)]*remaster[^)]*)\)", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\[([^\]]*remaster[^\]]*)\]", "", s, flags=re.IGNORECASE).strip()

    # 4) limpiar dobles espacios
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def pick_first_nonempty(series: pd.Series) -> str:
    for v in series.fillna("").astype(str).tolist():
        v = v.strip()
        if v:
            return v
    return ""


def fmt_year_int(val) -> str:
    """Convierte 1978.0 / '1978.0' / NaN / '' a '1978' o ''."""
    if val is None:
        return ""
    try:
        f = float(val)
        if f != f:  # NaN
            return ""
        return str(int(f))
    except Exception:
        s = str(val).strip()
        if not s or s.lower() == "nan":
            return ""
        return s.split(".")[0]


def owners_to_display(owners_val) -> str:
    """
    owners puede venir como:
    - 'Guille'
    - 'Guille|Colega 1|Colega 2'
    - '["Guille","Colega 1"]'
    - vacío
    Lo convertimos a algo compacto para header.
    """
    s = str(owners_val or "").strip()
    if not s or s.lower() == "nan":
        return ""

    # JSON list
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                arr = [str(x).strip() for x in arr if str(x).strip()]
                return ", ".join(arr)
        except Exception:
            pass

    # separadores típicos
    if "|" in s:
        parts = [p.strip() for p in s.split("|") if p.strip()]
        return ", ".join(parts)

    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expansion", default="I")
    ap.add_argument("--owner", default="Guille")  # se usa para localizar el linked
    ap.add_argument("--input-canonical", default=None)
    ap.add_argument("--input-linked", default=None)
    ap.add_argument("--output-csv", default=None)
    ap.add_argument("--output-json", default=None)
    args = ap.parse_args()

    expansion = args.expansion
    owner = args.owner

    in_canon = (
        Path(args.input_canonical)
        if args.input_canonical
        else (PROCESSED_DIR / f"canonical_songs_{expansion}_enriched.csv")
    )
    in_linked = (
        Path(args.input_linked)
        if args.input_linked
        else (PROCESSED_DIR / f"instances_linked_{expansion}_{owner}.csv")
    )

    out_csv = (
        Path(args.output_csv)
        if args.output_csv
        else (PROCESSED_DIR / f"deck_{expansion}.csv")
    )
    out_json = (
        Path(args.output_json)
        if args.output_json
        else (PROCESSED_DIR / f"deck_{expansion}.json")
    )

    if not in_canon.exists():
        raise FileNotFoundError(f"No existe: {in_canon}")
    if not in_linked.exists():
        raise FileNotFoundError(f"No existe: {in_linked}")

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    canon = pd.read_csv(in_canon).fillna("")
    linked = pd.read_csv(in_linked).fillna("")

    # Normalizar columnas esperadas (canon)
    for col in [
        "canonical_id",
        "title_canon",
        "artists_canon",
        "year",
        "first_seen_month",
        "first_seen_expansion",
    ]:
        if col not in canon.columns:
            canon[col] = ""

    # Normalizar columnas esperadas (linked)
    for col in [
        "canonical_id",
        "spotify_url",
        "spotify_uri",
        "track_id",
        "owner_label",
        "owners",
    ]:
        if col not in linked.columns:
            linked[col] = ""

    # Representante Spotify por canonical_id (para QR/link)
    rep = linked.groupby("canonical_id", as_index=False).agg(
        spotify_url=("spotify_url", pick_first_nonempty),
        spotify_uri=("spotify_uri", pick_first_nonempty),
        track_id=("track_id", pick_first_nonempty),
    )

    # Owners por canonical_id (para mostrar en la carta)
    # Preferimos 'owners' si existe; si no, agregamos owner_label únicos.
    if (
        "owners" in linked.columns
        and linked["owners"].astype(str).str.strip().ne("").any()
    ):
        owners_rep = linked.groupby("canonical_id", as_index=False).agg(
            owners=("owners", pick_first_nonempty)
        )
        owners_rep["owners"] = owners_rep["owners"].apply(owners_to_display)
    else:
        owners_rep = linked.groupby("canonical_id", as_index=False).agg(
            owners=(
                "owner_label",
                lambda s: " | ".join(
                    sorted({str(x).strip() for x in s.tolist() if str(x).strip()})
                ),
            )
        )
        owners_rep["owners"] = owners_rep["owners"].apply(owners_to_display)

    deck = canon[
        [
            "canonical_id",
            "title_canon",
            "artists_canon",
            "year",
            "first_seen_expansion",
            "first_seen_month",
        ]
    ].copy()

    deck = deck.merge(rep, on="canonical_id", how="left").fillna("")
    deck = deck.merge(owners_rep, on="canonical_id", how="left").fillna("")

    deck["expansion_code"] = expansion
    deck["card_id"] = (
        deck["expansion_code"].astype(str)
        + "-"
        + deck["canonical_id"].astype(str).str.slice(0, 8)
    )

    # Display (tu gusto)
    deck["title_display"] = deck["title_canon"].apply(clean_title_display)
    deck["artists_display"] = deck["artists_canon"].astype(str).str.strip()

    # Year como INT-string para carta (y mantener year original intacto si lo quieres)
    deck["year_int"] = deck["year"].apply(fmt_year_int)

    # Flags útiles
    deck["needs_manual_year"] = (deck["year_int"].astype(str).str.strip() == "").astype(
        int
    )
    deck["missing_spotify_url"] = (
        deck["spotify_url"].astype(str).str.strip() == ""
    ).astype(int)

    # Orden estable (para CSV)
    year_sort = (
        pd.to_numeric(deck["year_int"], errors="coerce").fillna(9999).astype(int)
    )
    deck["_year_sort"] = year_sort
    deck = deck.sort_values(
        ["_year_sort", "artists_display", "title_display", "canonical_id"]
    ).drop(columns=["_year_sort"])

    deck.to_csv(out_csv, index=False, encoding="utf-8")

    out_payload = deck.to_dict(orient="records")
    out_json.write_text(
        json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # QC report
    qc = {
        "cards": [len(deck)],
        "missing_year": [int(deck["needs_manual_year"].sum())],
        "missing_spotify_url": [int(deck["missing_spotify_url"].sum())],
        "dup_card_id": [int(deck["card_id"].duplicated().sum())],
        "dup_canonical_id": [int(deck["canonical_id"].duplicated().sum())],
    }
    qc_path = REPORTS_DIR / f"deck_qc_{expansion}.csv"
    pd.DataFrame(qc).to_csv(qc_path, index=False, encoding="utf-8")

    missing_spotify = deck[deck["missing_spotify_url"] == 1][
        ["canonical_id", "title_display", "artists_display", "owners"]
    ]
    missing_spotify_path = REPORTS_DIR / f"deck_missing_spotify_{expansion}.csv"
    missing_spotify.to_csv(missing_spotify_path, index=False, encoding="utf-8")

    print(f"OK deck -> {out_csv}")
    print(f"OK deck json -> {out_json}")
    print(f"OK qc -> {qc_path}")
    print(f"OK missing spotify -> {missing_spotify_path}")


if __name__ == "__main__":
    main()

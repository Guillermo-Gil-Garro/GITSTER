import argparse
import hashlib
from datetime import datetime
from pathlib import Path

import pandas as pd

PROCESSED_DIR = Path("pipeline/data/processed")
REGISTRY_DIR = Path("pipeline/registry")
MANUAL_DIR = Path("pipeline/manual")


def stable_hash_id(s: str, n: int = 16) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:n]


def load_manual_merges() -> pd.DataFrame:
    path = MANUAL_DIR / "manual_merges.csv"
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "alias_title_trim",
                "alias_artists_trim",
                "canonical_id_target",
                "note",
            ]
        )
    df = pd.read_csv(path).fillna("")
    # Normalizamos trims por seguridad (sin tocar raw)
    df["alias_title_trim"] = df["alias_title_trim"].astype(str).str.strip()
    df["alias_artists_trim"] = df["alias_artists_trim"].astype(str).str.strip()
    df["canonical_id_target"] = df["canonical_id_target"].astype(str).str.strip()
    return df


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

    in_path = PROCESSED_DIR / f"instances_{expansion}__{owner}.csv"
    if not in_path.exists():
        raise FileNotFoundError(f"No existe: {in_path}")

    df = pd.read_csv(in_path).fillna("")
    # Clave exacta (con trims) según decisión: auto-merge SOLO por match exacto
    df["title_trim"] = df["title_trim"].astype(str).str.strip()
    df["artists_trim"] = df["artists_trim"].astype(str).str.strip()
    df["canonical_key"] = df["artists_trim"] + " | " + df["title_trim"]

    # Canonical id por hash estable de canonical_key
    df["canonical_id"] = df["canonical_key"].apply(lambda x: stable_hash_id(x, 16))

    # Aplicar merges manuales (si existen): alias_title_trim+alias_artists_trim -> canonical_id_target
    merges = load_manual_merges()
    if len(merges) > 0:
        alias_key = merges["alias_artists_trim"] + " | " + merges["alias_title_trim"]
        alias_map = dict(zip(alias_key, merges["canonical_id_target"]))

        # solo sobreescribimos si hay canonical_id_target no vacío
        def apply_override(row):
            k = row["canonical_key"]
            target = alias_map.get(k, "")
            return target if target else row["canonical_id"]

        df["canonical_id"] = df.apply(apply_override, axis=1)

    # canonical_songs: únicos por canonical_id (tomamos title/artists trim como canon)
    canonical = (
        df[["canonical_id", "title_trim", "artists_trim"]]
        .drop_duplicates(subset=["canonical_id"])
        .rename(columns={"title_trim": "title_canon", "artists_trim": "artists_canon"})
        .copy()
    )
    canonical["year"] = ""  # se rellenará por enriquecimiento externo
    canonical["first_seen_expansion"] = expansion
    canonical["first_seen_month"] = processed_month

    # Guardar outputs
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)

    canonical_path = PROCESSED_DIR / f"canonical_songs_{expansion}.csv"
    linked_path = PROCESSED_DIR / f"instances_linked_{expansion}__{owner}.csv"
    registry_path = REGISTRY_DIR / "canonical_registry.csv"

    canonical.to_csv(canonical_path, index=False, encoding="utf-8")
    df.to_csv(linked_path, index=False, encoding="utf-8")

    # Actualizar registry (append sin duplicar canonical_id)
    if registry_path.exists():
        reg = pd.read_csv(registry_path).fillna("")
    else:
        reg = pd.DataFrame(columns=["canonical_id", "expansion_code", "added_month"])

    new_reg = pd.DataFrame(
        {
            "canonical_id": canonical["canonical_id"],
            "expansion_code": expansion,
            "added_month": processed_month,
        }
    )

    reg_all = pd.concat([reg, new_reg], ignore_index=True)
    reg_all = reg_all.drop_duplicates(subset=["canonical_id"], keep="first")
    reg_all.to_csv(registry_path, index=False, encoding="utf-8")

    print(f"OK canonical -> {canonical_path} ({len(canonical)} songs)")
    print(f"OK linked -> {linked_path} ({len(df)} instances)")
    print(f"OK registry -> {registry_path} ({len(reg_all)} total unique songs)")


if __name__ == "__main__":
    main()

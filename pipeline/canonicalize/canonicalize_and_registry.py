import argparse
import hashlib
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, List

import pandas as pd


def repo_root() -> Path:
    # .../pipeline/canonicalize/canonicalize_and_registry.py -> parents[2] = repo root
    return Path(__file__).resolve().parents[2]


ROOT = repo_root()
PROCESSED_DIR = ROOT / "pipeline" / "data" / "processed"
REPORTS_DIR = ROOT / "pipeline" / "reports"
MANUAL_DIR = ROOT / "pipeline" / "manual"
REGISTRY_DIR = ROOT / "pipeline" / "registry"

MANUAL_MERGES_PATH = MANUAL_DIR / "manual_merges.csv"
REGISTRY_PATH = REGISTRY_DIR / "canonical_registry.csv"


def norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip()
    s = " ".join(s.split())
    return s


def norm_key(artists_trim: str, title_trim: str) -> str:
    # Normalización estable para hashing y matching de merges manuales
    return f"{norm(artists_trim).lower()} | {norm(title_trim).lower()}"


def canonical_id_from_key(key_norm: str) -> str:
    # 16 hex (compatible con ids existentes en el repo)
    return hashlib.md5(key_norm.encode("utf-8")).hexdigest()[:16]


def list_owners_from_instances(expansion: str) -> List[str]:
    pattern = f"instances_{expansion}_*.csv"
    files = sorted(PROCESSED_DIR.glob(pattern))
    owners: List[str] = []
    prefix = f"instances_{expansion}_"
    for f in files:
        stem = f.stem  # instances_I_Guille
        if not stem.startswith(prefix):
            continue
        owner = stem[len(prefix):]
        owners.append(owner)
    # dedup manteniendo orden
    seen = set()
    out = []
    for o in owners:
        if o not in seen:
            seen.add(o)
            out.append(o)
    return out


def load_manual_merges() -> Dict[str, str]:
    """
    manual_merges.csv:
      alias_title_trim, alias_artists_trim, canonical_id_target, note
    """
    if not MANUAL_MERGES_PATH.exists():
        return {}
    df = pd.read_csv(MANUAL_MERGES_PATH).fillna("")
    if df.empty:
        return {}
    required = {"alias_title_trim", "alias_artists_trim", "canonical_id_target"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"manual_merges.csv debe tener columnas {sorted(required)}")
    mapping: Dict[str, str] = {}
    for r in df.itertuples(index=False):
        k = norm_key(getattr(r, "alias_artists_trim", ""), getattr(r, "alias_title_trim", ""))
        tgt = norm(getattr(r, "canonical_id_target", ""))
        if not k or not tgt:
            continue
        mapping[k] = tgt
    return mapping


def choose_mode(values: List[str]) -> str:
    vals = [norm(v) for v in values if norm(v)]
    if not vals:
        return ""
    c = Counter(vals)
    # mode; tie-break: lexicográfico
    top = max(c.values())
    best = sorted([v for v, n in c.items() if n == top])[0]
    return best


def update_registry(canonical_ids: List[str], expansion: str, added_month: str) -> None:
    REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
    if REGISTRY_PATH.exists():
        reg = pd.read_csv(REGISTRY_PATH).fillna("")
    else:
        reg = pd.DataFrame(columns=["canonical_id", "expansion_code", "added_month"])

    reg_cols = set(reg.columns)
    required = {"canonical_id", "expansion_code", "added_month"}
    if not required.issubset(reg_cols):
        # No rompemos: re-creamos con required + lo que hubiera
        reg = reg[[c for c in reg.columns if c in required]].copy()

    existing = set(reg.loc[reg["expansion_code"] == expansion, "canonical_id"].astype(str))
    new_rows = []
    for cid in canonical_ids:
        cid = norm(cid)
        if not cid or cid in existing:
            continue
        new_rows.append({"canonical_id": cid, "expansion_code": expansion, "added_month": added_month})

    if new_rows:
        reg = pd.concat([reg, pd.DataFrame(new_rows)], ignore_index=True)

    reg.to_csv(REGISTRY_PATH, index=False)


def canonicalize_owner(owner: str, expansion: str, merges: Dict[str, str]) -> pd.DataFrame:
    in_path = PROCESSED_DIR / f"instances_{expansion}_{owner}.csv"
    if not in_path.exists():
        raise FileNotFoundError(f"No existe: {in_path}")

    df = pd.read_csv(in_path).fillna("")
    # columnas mínimas esperadas del build_instances.py
    needed = {"owner_label", "title_trim", "artists_trim"}
    if not needed.issubset(set(df.columns)):
        raise ValueError(f"{in_path.name} debe tener columnas al menos: {sorted(needed)}")

    # Canonical key visible (mantén case trim original)
    df["canonical_key"] = df["artists_trim"].astype(str).apply(norm) + " | " + df["title_trim"].astype(str).apply(norm)

    # Canonical id estable (hash del key normalizado)
    key_norm = df.apply(lambda r: norm_key(r.get("artists_trim", ""), r.get("title_trim", "")), axis=1)
    df["canonical_id"] = key_norm.apply(canonical_id_from_key)

    # merges manuales (por alias)
    if merges:
        mask = key_norm.isin(merges.keys())
        if mask.any():
            df.loc[mask, "canonical_id"] = key_norm[mask].map(merges)

    # orden columnas: añade al final si ya existe estructura
    # (no forzamos un schema rígido para no romper downstream)
    return df


def build_canonical_songs(linked_all: pd.DataFrame, expansion: str) -> pd.DataFrame:
    # Preferimos title_trim/artists_trim como canon
    if "title_trim" not in linked_all.columns or "artists_trim" not in linked_all.columns:
        raise ValueError("linked instances deben incluir title_trim y artists_trim para construir canonical_songs")

    g = linked_all.groupby("canonical_id", dropna=False)

    rows = []
    for cid, sub in g:
        title_c = choose_mode(sub["title_trim"].astype(str).tolist())
        artists_c = choose_mode(sub["artists_trim"].astype(str).tolist())
        rows.append({
            "canonical_id": cid,
            "title_canon": title_c,
            "artists_canon": artists_c,
            "year": "",
            "year_source": "",
            "year_confidence": "",
            "year_note": "",
        })

    out = pd.DataFrame(rows)
    out.insert(0, "expansion_code", expansion)
    return out.sort_values(["artists_canon", "title_canon"]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--owner", help="Procesa un owner específico (si no se indica, procesa todos los instances_{EXP}_*.csv)", default=None)
    parser.add_argument("--expansion", required=True, help="Código de expansión (ej: I)")
    parser.add_argument("--processed-month", default=None, help="YYYY-MM (si no, se infiere del CSV o se usa mes actual)")

    args = parser.parse_args()
    expansion = args.expansion

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    owners = [args.owner] if args.owner else list_owners_from_instances(expansion)
    if not owners:
        raise FileNotFoundError(f"No encuentro instances_{expansion}_*.csv en {PROCESSED_DIR}")

    merges = load_manual_merges()

    linked_frames = []
    processed_month = args.processed_month

    for owner in owners:
        linked = canonicalize_owner(owner, expansion, merges)
        # infer processed_month si no se pasó
        if not processed_month:
            if "processed_month" in linked.columns:
                vals = sorted({norm(v) for v in linked["processed_month"].tolist() if norm(v)})
                if len(vals) == 1:
                    processed_month = vals[0]
                elif len(vals) > 1:
                    processed_month = max(vals)
        linked_out = PROCESSED_DIR / f"instances_linked_{expansion}_{owner}.csv"
        linked.to_csv(linked_out, index=False)
        print(f"OK linked -> {linked_out}")

        linked_frames.append(linked)

    if not processed_month:
        processed_month = datetime.now().strftime("%Y-%m")

    linked_all = pd.concat(linked_frames, ignore_index=True)
    canonical_songs = build_canonical_songs(linked_all, expansion)
    canon_out = PROCESSED_DIR / f"canonical_songs_{expansion}.csv"
    canonical_songs.to_csv(canon_out, index=False)
    print(f"OK canonical_songs -> {canon_out}")

    # QC básico
    qc = pd.DataFrame({
        "expansion": [expansion],
        "owners": [", ".join(owners)],
        "n_instances": [len(linked_all)],
        "n_canonical": [linked_all["canonical_id"].nunique()],
        "processed_month": [processed_month],
        "n_manual_merges": [len(merges)],
    })
    qc_out = REPORTS_DIR / f"qc_canonicalize_{expansion}.csv"
    qc.to_csv(qc_out, index=False)
    print(f"OK qc -> {qc_out}")

    # registry
    update_registry(sorted(linked_all["canonical_id"].unique().tolist()), expansion, processed_month)
    print(f"OK registry -> {REGISTRY_PATH}")


if __name__ == "__main__":
    main()

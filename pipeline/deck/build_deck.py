import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

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


def parse_owners_field(v) -> List[str]:
    """
    Acepta owners como:
    - 'Guille'
    - 'Guille|Luks'
    - 'Guille, Luks'
    - '["Guille","Luks"]'
    """
    s = str(v or "").strip()
    if not s or s.lower() == "nan":
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                out = [str(x).strip() for x in arr if str(x).strip()]
                return out
        except Exception:
            pass

    if "|" in s:
        return [p.strip() for p in s.split("|") if p.strip()]
    if "," in s:
        return [p.strip() for p in s.split(",") if p.strip()]
    return [s]


def owners_to_display(owners: Iterable[str]) -> str:
    arr = [str(x).strip() for x in owners if str(x).strip()]
    arr = sorted(set(arr), key=lambda x: x.lower())
    return ", ".join(arr)


def load_linked_instances(
    expansion: str,
    owner_legacy: str,
    input_linked: Optional[str],
) -> pd.DataFrame:
    """
    Carga instancias 'linked' (instances_linked_<EXP>_<OWNER>.csv).
    - Si --input-linked apunta a fichero: usa ese.
    - Si --input-linked apunta a directorio: carga todos los CSV dentro.
    - Si --input-linked contiene wildcard (* o ?): glob.
    - Si no se pasa --input-linked: autodetecta todos los instances_linked_<EXP>_*.csv;
      si no encuentra, cae a la ruta legacy instances_linked_<EXP>_<owner>.csv.
    """
    def read_one(p: Path) -> pd.DataFrame:
        d = pd.read_csv(p).fillna("")
        d["_source_file"] = p.name
        return d

    if input_linked:
        p = Path(input_linked)
        if any(ch in str(input_linked) for ch in ["*", "?"]):
            files = sorted(Path().glob(str(input_linked)))
            if not files:
                raise FileNotFoundError(f"No hay coincidencias para glob: {input_linked}")
            return pd.concat([read_one(f) for f in files], ignore_index=True)

        if p.is_dir():
            files = sorted(p.glob("*.csv"))
            if not files:
                raise FileNotFoundError(f"No hay CSV en directorio: {p}")
            return pd.concat([read_one(f) for f in files], ignore_index=True)

        if p.is_file():
            return read_one(p)

        raise FileNotFoundError(f"No existe --input-linked: {p}")

    # autodetect
    pattern = PROCESSED_DIR / f"instances_linked_{expansion}_*.csv"
    files = sorted(pattern.parent.glob(pattern.name))
    if files:
        return pd.concat([read_one(f) for f in files], ignore_index=True)

    # legacy
    legacy = PROCESSED_DIR / f"instances_linked_{expansion}_{owner_legacy}.csv"
    if legacy.exists():
        return read_one(legacy)

    raise FileNotFoundError(
        f"No se encontraron linked instances. Probé:\n"
        f"- {pattern}\n- {legacy}\n"
        f"Usa --input-linked para apuntar a un CSV/directorio/glob."
    )


def compute_owner_cap_cards(
    limit: int,
    owners: List[str],
    owner_cap_percent: float,
    owner_cap_cards: int,
    owner_cap_slack: float,
) -> int:
    if owner_cap_cards and owner_cap_cards > 0:
        return int(owner_cap_cards)

    if owner_cap_percent and owner_cap_percent > 0:
        return int(math.ceil(limit * float(owner_cap_percent)))

    n = max(1, len(owners))
    # Por defecto: 1/N + slack (clamp para que no sea ridículo con N grande)
    pct = (1.0 / n) + float(owner_cap_slack)
    pct = max(pct, 0.12)  # mínimo 12%
    pct = min(pct, 0.85)  # máximo 85%
    return int(math.ceil(limit * pct))


def choose_primary_owner(owners_sorted: List[str], owner_counts: Dict[str, int]) -> str:
    if not owners_sorted:
        return ""
    # El menos representado; desempate alfabético
    return sorted(owners_sorted, key=lambda o: (owner_counts.get(o, 0), o.lower()))[0]


def select_deck_300(
    df: pd.DataFrame,
    limit: int,
    max_per_album: int,
    owner_cap_cards: int,
    relax_owner_cap_if_needed: bool,
    prefer_have_spotify_url: bool,
    reports_prefix: Path,
) -> pd.DataFrame:
    """
    Selección con:
    - Cobertura: 1 por año (siempre que year != NaN)
    - Prioridad: owners_count desc
    - Equidad: soft cap por owner (primary_owner)
    - Cap álbum: max_per_album por album_id
    """
    if limit <= 0:
        return df.copy()

    # Validaciones mínimas
    if "year" not in df.columns:
        raise ValueError("El DF de entrada no tiene columna 'year'.")

    # Año int (hard)
    year_int = pd.to_numeric(df["year"], errors="coerce")
    missing_year = year_int.isna()
    if missing_year.any():
        miss = df.loc[missing_year, ["canonical_id", "title_display", "artists_display"]].copy()
        miss_path = reports_prefix / "deck_year_missing.csv"
        miss.to_csv(miss_path, index=False, encoding="utf-8")
        raise ValueError(
            f"Hay {int(missing_year.sum())} canciones sin year resuelto. "
            f"Resuelve años antes de montar deck. Ver: {miss_path}"
        )
    df = df.copy()
    df["year"] = year_int.astype(int)


    # Album cap (si aplica): preferimos album_id; fallback a album_name + year si no existe.
    if max_per_album > 0:
        df = df.copy()

        # Normalizar posibles columnas
        if "album_id" in df.columns:
            df["album_id"] = df["album_id"].astype(str).str.strip()
        if "album_name_trim" in df.columns:
            df["album_name_trim"] = df["album_name_trim"].astype(str).str.strip()
        if "album_name_raw" in df.columns:
            df["album_name_raw"] = df["album_name_raw"].astype(str).str.strip()
        if "album_name" in df.columns:
            df["album_name"] = df["album_name"].astype(str).str.strip()

        def _norm_album_name(s: str) -> str:
            s = str(s or "").strip().lower()
            s = re.sub(r"\s{2,}", " ", s)
            return s

        # Construir album_key
        df["album_key"] = ""

        # 1) album_id
        if "album_id" in df.columns and df["album_id"].ne("").any():
            mask_id = df["album_id"].ne("")
            df.loc[mask_id, "album_key"] = "id:" + df.loc[mask_id, "album_id"]

        # 2) fallback a album_name_* (incluyendo year para evitar colisiones)
        if df["album_key"].eq("").any():
            name_series = None
            for c in ["album_name_trim", "album_name", "album_name_raw"]:
                if c in df.columns and df[c].astype(str).str.strip().ne("").any():
                    name_series = df[c].astype(str)
                    break

            if name_series is not None:
                mask = df["album_key"].eq("") & name_series.astype(str).str.strip().ne("")
                df.loc[mask, "album_key"] = (
                    "name:" + name_series.loc[mask].map(_norm_album_name) + "::" + df.loc[mask, "year"].astype(int).astype(str)
                )

        if df["album_key"].astype(str).str.strip().eq("").all():
            miss = df.loc[:, ["canonical_id", "title_display", "artists_display", "spotify_url"]].copy()
            miss_path = reports_prefix / "deck_album_missing.csv"
            miss.to_csv(miss_path, index=False, encoding="utf-8")
            raise ValueError(
                "Has pedido cap de álbum (--max-per-album > 0) pero no hay 'album_id' ni 'album_name' usable. "
                "Necesitamos propagar album_id (ideal) o al menos album_name (fallback) hasta el dataset. "
                f"Ver ejemplo en: {miss_path}"
            )


    # Owners list (hard)
    if "owners_list" not in df.columns:
        raise ValueError("Falta columna interna owners_list.")
    df["owners_list"] = df["owners_list"].apply(lambda x: x if isinstance(x, list) else parse_owners_field(x))
    df["owners_list"] = df["owners_list"].apply(lambda arr: sorted(set([str(o).strip() for o in arr if str(o).strip()]), key=lambda s: s.lower()))
    df["owners_count"] = df["owners_list"].apply(len)

    # Preferimos que haya spotify_url si se pide
    if "spotify_url" in df.columns:
        _surl = df["spotify_url"]
    else:
        _surl = pd.Series([""] * len(df), index=df.index)
    df["has_spotify_url"] = _surl.astype(str).str.strip().ne("").astype(int)

    owners_universe = sorted(set([o for arr in df["owners_list"].tolist() for o in arr]), key=lambda s: s.lower())
    owner_counts: Dict[str, int] = {o: 0 for o in owners_universe}
    album_counts: Dict[str, int] = defaultdict(int)

    selected_ids: List[str] = []
    selected_rows: List[dict] = []
    violations = {
        "album_cap_violations": 0,
        "owner_cap_violations": 0,
    }

    def can_take(row, enforce_owner_cap=True, enforce_album_cap=True) -> bool:
        cid = row["canonical_id"]
        if cid in selected_ids:
            return False

        if max_per_album > 0 and enforce_album_cap:
            aid = str(row.get("album_key", "")).strip()
            if aid and album_counts[aid] >= max_per_album:
                return False

        if enforce_owner_cap and owner_cap_cards > 0:
            po = choose_primary_owner(row["owners_list"], owner_counts)
            if po and owner_counts.get(po, 0) >= owner_cap_cards:
                return False

        return True

    def add_row(row, note_violation_owner=False, note_violation_album=False) -> None:
        cid = row["canonical_id"]
        selected_ids.append(cid)

        po = choose_primary_owner(row["owners_list"], owner_counts)
        if po:
            owner_counts[po] = owner_counts.get(po, 0) + 1

        if max_per_album > 0:
            aid = str(row.get("album_key", "")).strip()
            if aid:
                album_counts[aid] += 1

        r = dict(row)
        r["primary_owner"] = po
        if note_violation_owner:
            r["_violation_owner_cap"] = 1
            violations["owner_cap_violations"] += 1
        else:
            r["_violation_owner_cap"] = 0
        if note_violation_album:
            r["_violation_album_cap"] = 1
            violations["album_cap_violations"] += 1
        else:
            r["_violation_album_cap"] = 0
        selected_rows.append(r)

    # Orden base de preferencia
    sort_cols = ["owners_count", "has_spotify_url", "year", "artists_display", "title_display", "canonical_id"]
    df_sorted = df.sort_values(sort_cols, ascending=[False, False, True, True, True, True])

    # 1) Cobertura: 1 por año
    years = sorted(df_sorted["year"].unique().tolist())
    for y in years:
        if len(selected_ids) >= limit:
            break
        cands = df_sorted[df_sorted["year"] == y]
        # Intento 1: respetando caps
        picked = None
        for _, row in cands.iterrows():
            if can_take(row, enforce_owner_cap=True, enforce_album_cap=True):
                picked = row
                break
        if picked is None:
            # Intento 2: relajar owner cap para asegurar año
            for _, row in cands.iterrows():
                if can_take(row, enforce_owner_cap=False, enforce_album_cap=True):
                    picked = row
                    add_row(picked, note_violation_owner=True, note_violation_album=False)
                    break
        if picked is None:
            # Intento 3: relajar album cap (último recurso) para asegurar año
            for _, row in cands.iterrows():
                if can_take(row, enforce_owner_cap=False, enforce_album_cap=False):
                    picked = row
                    add_row(picked, note_violation_owner=True, note_violation_album=True)
                    break
        if picked is not None and picked["canonical_id"] not in selected_ids:
            add_row(picked)

    # 2) Relleno hasta limit con caps (y relax opcional de owner cap si nos atascamos)
    def fill(pass_enforce_owner: bool) -> int:
        added = 0
        for _, row in df_sorted.iterrows():
            if len(selected_ids) >= limit:
                break
            if can_take(row, enforce_owner_cap=pass_enforce_owner, enforce_album_cap=True):
                add_row(row, note_violation_owner=(not pass_enforce_owner), note_violation_album=False)
                added += 1
        return added

    # pass 1: caps estrictos
    fill(pass_enforce_owner=True)

    if len(selected_ids) < limit and relax_owner_cap_if_needed and owner_cap_cards > 0:
        # pass 2: relajar owner cap (manteniendo album cap)
        fill(pass_enforce_owner=False)

    out = pd.DataFrame(selected_rows)

    # Reporte de selección
    out_report = out[[
        "canonical_id", "card_id", "year", "owners_count", "primary_owner", "album_id",
        "_violation_owner_cap", "_violation_album_cap", "title_display", "artists_display", "spotify_url", "owners"
    ]].copy() if "album_id" in out.columns else out[[
        "canonical_id", "card_id", "year", "owners_count", "primary_owner",
        "_violation_owner_cap", "_violation_album_cap", "title_display", "artists_display", "spotify_url", "owners"
    ]].copy()
    sel_path = reports_prefix / "deck_selection.csv"
    out_report.to_csv(sel_path, index=False, encoding="utf-8")

    meta = {
        "limit": limit,
        "selected": int(len(out)),
        "years_total": int(len(years)),
        "years_covered": int(out["year"].nunique()) if len(out) else 0,
        "owners_universe": owners_universe,
        "owner_cap_cards": int(owner_cap_cards),
        "max_per_album": int(max_per_album),
        **violations,
        "owner_counts_primary": dict(sorted(owner_counts.items(), key=lambda kv: (-kv[1], kv[0].lower()))),
    }
    (reports_prefix / "deck_selection_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if len(out) < limit:
        # No lo hacemos fatal: dejamos el deck parcial pero avisamos
        warn_path = reports_prefix / "deck_selection_warning.txt"
        warn_path.write_text(
            f"No se pudo llenar hasta {limit}. Seleccionadas: {len(out)}.\n"
            f"Revisa caps (owner_cap/album_cap) o incrementa slack.\n",
            encoding="utf-8",
        )

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expansion", default="I")

    # Legacy: se mantiene para localizar instances_linked_<exp>_<owner>.csv si no hay autodetect
    ap.add_argument("--owner", default="Guille")

    ap.add_argument("--input-canonical", default=None)
    ap.add_argument("--input-linked", default=None)

    ap.add_argument("--output-csv", default=None)
    ap.add_argument("--output-json", default=None)

    # NUEVO: build de deck limitado + constraints
    ap.add_argument("--limit", type=int, default=300, help="Número de cartas del deck (0 = sin límite)")
    ap.add_argument("--prefer-have-spotify-url", action="store_true", help="En desempates, prioriza que haya spotify_url")
    ap.add_argument("--max-per-album", type=int, default=3, help="Máximo de cartas por album_id (0 = desactivar)")
    ap.add_argument("--owner-cap-percent", type=float, default=0.0, help="Cap por owner como porcentaje (0 = auto)")
    ap.add_argument("--owner-cap-cards", type=int, default=0, help="Cap por owner en nº de cartas (0 = auto)")
    ap.add_argument("--owner-cap-slack", type=float, default=0.12, help="Slack sumado a 1/N en modo auto (ej: 0.12)")
    ap.add_argument("--relax-owner-cap-if-needed", action="store_true", help="Si no llena, relaja owner cap (mantiene album cap)")

    args = ap.parse_args()

    expansion = args.expansion
    owner_legacy = args.owner

    in_canon = (
        Path(args.input_canonical)
        if args.input_canonical
        else (PROCESSED_DIR / f"canonical_songs_{expansion}_enriched.csv")
    )

    if not in_canon.exists():
        raise FileNotFoundError(f"No existe: {in_canon}")

    linked = load_linked_instances(
        expansion=expansion,
        owner_legacy=owner_legacy,
        input_linked=args.input_linked,
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

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    canon = pd.read_csv(in_canon).fillna("")

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
        "album_id",
        "album_name",
    ]:
        if col not in linked.columns:
            linked[col] = ""

    canon["canonical_id"] = canon["canonical_id"].astype(str).str.strip()
    linked["canonical_id"] = linked["canonical_id"].astype(str).str.strip()

    # Representante Spotify por canonical_id (para QR/link) + álbum si existe
    agg_dict = {
        "spotify_url": ("spotify_url", pick_first_nonempty),
        "spotify_uri": ("spotify_uri", pick_first_nonempty),
        "track_id": ("track_id", pick_first_nonempty),
    }
    if "album_id" in linked.columns:
        agg_dict["album_id"] = ("album_id", pick_first_nonempty)
    if "album_name" in linked.columns:
        agg_dict["album_name"] = ("album_name", pick_first_nonempty)

    rep = linked.groupby("canonical_id", as_index=False).agg(**agg_dict)

    # Owners por canonical_id: siempre agregamos owner_label únicos
    owners_rep = linked.groupby("canonical_id", as_index=False).agg(
        owners_list=("owner_label", lambda s: sorted({str(x).strip() for x in s.tolist() if str(x).strip()}, key=lambda x: x.lower()))
    )
    owners_rep["owners"] = owners_rep["owners_list"].apply(owners_to_display)

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
    deck = deck.merge(owners_rep, on="canonical_id", how="left")

    # IMPORTANTE: no usar fillna con listas (pandas no lo soporta). Normalizamos columna a columna.
    if "owners_list" not in deck.columns:
        deck["owners_list"] = [[] for _ in range(len(deck))]
    else:
        deck["owners_list"] = deck["owners_list"].apply(lambda x: x if isinstance(x, list) else [])
    # owners string siempre derivado y ordenado
    deck["owners"] = deck["owners_list"].apply(owners_to_display)


    deck["expansion_code"] = expansion
    deck["card_id"] = deck["expansion_code"].astype(str) + "-" + deck["canonical_id"].astype(str).str.slice(0, 8)

    # Display
    deck["title_display"] = deck["title_canon"].apply(clean_title_display)
    deck["artists_display"] = deck["artists_canon"].astype(str).str.strip()

    # Year como INT-string para carta (y mantener year original si lo quieres)
    deck["year_int"] = deck["year"].apply(fmt_year_int)

    # Album fallback desde canon si existiera
    if "album_id" not in deck.columns and "album_id" in canon.columns:
        deck["album_id"] = canon["album_id"].astype(str).str.strip()
    if "album_name" not in deck.columns and "album_name" in canon.columns:
        deck["album_name"] = canon["album_name"].astype(str).str.strip()

    # Flags útiles
    deck["needs_manual_year"] = (deck["year_int"].astype(str).str.strip() == "").astype(int)
    if "spotify_url" in deck.columns:
        _surl2 = deck["spotify_url"]
    else:
        _surl2 = pd.Series([""] * len(deck), index=deck.index)
    deck["missing_spotify_url"] = _surl2.astype(str).str.strip().eq("").astype(int)

    # Orden estable base (para modo sin limit)
    year_sort = pd.to_numeric(deck["year_int"], errors="coerce").fillna(9999).astype(int)
    deck["_year_sort"] = year_sort

    # Selección con constraints
    limit = int(args.limit or 0)
    prefer_have_spotify = bool(args.prefer_have_spotify_url)

    # Cap owner (auto)
    owners_universe = sorted(set([o for arr in deck["owners_list"].tolist() for o in (arr if isinstance(arr, list) else [])]), key=lambda s: s.lower())
    owner_cap_cards = compute_owner_cap_cards(
        limit=limit if limit > 0 else max(1, len(deck)),
        owners=owners_universe,
        owner_cap_percent=float(args.owner_cap_percent or 0.0),
        owner_cap_cards=int(args.owner_cap_cards or 0),
        owner_cap_slack=float(args.owner_cap_slack or 0.0),
    )

    # Prefijo reports por expansión
    reports_prefix = REPORTS_DIR / f"deck_build_{expansion}"
    reports_prefix.mkdir(parents=True, exist_ok=True)

    if limit > 0:
        selected = select_deck_300(
            df=deck,
            limit=limit,
            max_per_album=int(args.max_per_album or 0),
            owner_cap_cards=int(owner_cap_cards),
            relax_owner_cap_if_needed=bool(args.relax_owner_cap_if_needed),
            prefer_have_spotify_url=prefer_have_spotify,
            reports_prefix=reports_prefix,
        )
        deck_out = selected.copy()
    else:
        deck_out = deck.sort_values(
            ["_year_sort", "artists_display", "title_display", "canonical_id"]
        ).copy()

    if "_year_sort" in deck_out.columns:
        deck_out = deck_out.drop(columns=["_year_sort"], errors="ignore")
    if "_year_sort" in deck.columns:
        deck = deck.drop(columns=["_year_sort"], errors="ignore")

    # Año final: forzamos year int si se seleccionó
    if "year" in deck_out.columns:
        # si viene como string, intenta
        y = pd.to_numeric(deck_out["year"], errors="coerce")
        if y.notna().all():
            deck_out["year"] = y.astype(int)

    # CSV/JSON
    deck_out.to_csv(out_csv, index=False, encoding="utf-8")
    out_payload = deck_out.to_dict(orient="records")
    out_json.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # QC report
    qc = {
        "cards": [len(deck_out)],
        "limit": [limit],
        "unique_years_in_deck": [int(pd.to_numeric(deck_out.get("year", ""), errors="coerce").nunique()) if len(deck_out) else 0],
        "missing_year": [int(deck_out.get("needs_manual_year", pd.Series(dtype=int)).sum()) if "needs_manual_year" in deck_out.columns else 0],
        "missing_spotify_url": [int(deck_out.get("missing_spotify_url", pd.Series(dtype=int)).sum()) if "missing_spotify_url" in deck_out.columns else 0],
        "dup_card_id": [int(deck_out["card_id"].duplicated().sum())],
        "dup_canonical_id": [int(deck_out["canonical_id"].duplicated().sum())],
        "owners_universe": [", ".join(owners_universe)],
        "owner_cap_cards": [int(owner_cap_cards) if limit > 0 else 0],
        "max_per_album": [int(args.max_per_album or 0) if limit > 0 else 0],
    }
    qc_path = REPORTS_DIR / f"deck_qc_{expansion}.csv"
    pd.DataFrame(qc).to_csv(qc_path, index=False, encoding="utf-8")

    missing_spotify = deck_out[deck_out.get("missing_spotify_url", 0) == 1][
        ["canonical_id", "title_display", "artists_display", "owners"]
    ] if "missing_spotify_url" in deck_out.columns else pd.DataFrame(columns=["canonical_id","title_display","artists_display","owners"])
    missing_spotify_path = REPORTS_DIR / f"deck_missing_spotify_{expansion}.csv"
    missing_spotify.to_csv(missing_spotify_path, index=False, encoding="utf-8")

    print(f"OK deck -> {out_csv}")
    print(f"OK deck json -> {out_json}")
    print(f"OK qc -> {qc_path}")
    print(f"OK missing spotify -> {missing_spotify_path}")
    if limit > 0:
        print(f"OK selection reports -> {reports_prefix}")


if __name__ == "__main__":
    main()

import argparse
import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

PROCESSED_DIR = Path("pipeline/data/processed")
REPORTS_DIR = Path("pipeline/reports")
MANUAL_DIR = Path("pipeline/manual")

FEAT_BRACKET_RE = re.compile(r"[\(\[]\s*(feat\.?|ft\.?|featuring)\b.*?[\)\]]", flags=re.IGNORECASE)
FEAT_INLINE_RE = re.compile(r"\b(feat\.?|ft\.?|featuring)\b.*$", flags=re.IGNORECASE)
PUNCT_RE = re.compile(r"[^a-z0-9\s]")
SPACE_RE = re.compile(r"\s+")

VERSION_TAG_PATTERNS = [
    r"\bremaster(ed)?\b",
    r"\bradio\s+edit\b",
    r"\bedit\b",
    r"\blive\b",
    r"\bacoustic\b",
    r"\bdemo\b",
    r"\binstrumental\b",
    r"\bextended\b",
    r"\bversion\b",
    r"\bmono\b",
    r"\bstereo\b",
    r"\bclean\b",
    r"\bexplicit\b",
    r"\breissue\b",
]

KEEP_DISTINCT_PATTERNS = [
    r"\bremix\b",
    r"\bsped\s+up\b",
    r"\bspeed\s+up\b",
    r"\bslowed\b",
    r"\bnightcore\b",
]

TITLE_STOPWORDS = {
    "a",
    "al",
    "an",
    "and",
    "as",
    "at",
    "by",
    "con",
    "da",
    "das",
    "de",
    "del",
    "di",
    "do",
    "dos",
    "e",
    "el",
    "en",
    "for",
    "in",
    "la",
    "las",
    "los",
    "of",
    "on",
    "or",
    "para",
    "por",
    "the",
    "to",
    "u",
    "un",
    "una",
    "unos",
    "unas",
    "y",
}

WORD_TOKEN_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9]+(?:'[A-Za-zÀ-ÖØ-öø-ÿ0-9]+)?")
DASH_WITH_SPACES_RE = re.compile(r"\s+[–—-]\s+")

ACRONYM_KEEP = {
    "DJ",
    "EP",
    "LP",
    "OST",
    "TV",
    "UK",
    "USA",
}

ROMAN_NUMERALS_KEEP = {
    "I",
    "II",
    "III",
    "IV",
    "V",
    "VI",
    "VII",
    "VIII",
    "IX",
    "X",
    "XI",
    "XII",
    "XIII",
    "XIV",
    "XV",
    "XVI",
    "XVII",
    "XVIII",
    "XIX",
    "XX",
}


def pick_first_nonempty(series: pd.Series) -> str:
    for value in series.fillna("").astype(str).tolist():
        value = value.strip()
        if value:
            return value
    return ""


def parse_year_int(value: object) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        as_float = float(text)
    except Exception:
        return None
    if math.isnan(as_float):
        return None
    return int(as_float)


def parse_float(value: object, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    if math.isnan(out):
        return default
    return out


def parse_owners_field(value: object) -> List[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return []

    if text.startswith("[") and text.endswith("]"):
        try:
            arr = json.loads(text)
            if isinstance(arr, list):
                vals = [str(v).strip() for v in arr if str(v).strip()]
                return sorted(set(vals), key=lambda s: s.lower())
        except Exception:
            pass

    for sep in ["|", ";", "/", "\\"]:
        text = text.replace(sep, ",")
    vals = [v.strip() for v in text.split(",") if v.strip()]
    return sorted(set(vals), key=lambda s: s.lower())


def owners_to_display(owners: Iterable[str]) -> str:
    vals = sorted({str(o).strip() for o in owners if str(o).strip()}, key=lambda s: s.lower())
    return ", ".join(vals)


def normalize_quotes_and_spaces(text: str) -> str:
    out = str(text or "")
    replacements = {
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u00b4": "'",
        "\u0060": "'",
        "\u00a0": " ",
        "\u2007": " ",
        "\u202f": " ",
        "\u2009": " ",
        "\u200a": " ",
        "\u200b": " ",
    }
    for src, dst in replacements.items():
        out = out.replace(src, dst)
    out = SPACE_RE.sub(" ", out).strip()
    return out


def normalize_dash_separator(text: str) -> str:
    out = str(text or "")
    out = DASH_WITH_SPACES_RE.sub(" - ", out)
    out = SPACE_RE.sub(" ", out).strip()
    return out


def mostly_upper_or_lower(text: str) -> bool:
    letters = [ch for ch in str(text or "") if ch.isalpha()]
    if not letters:
        return False
    upper = sum(1 for ch in letters if ch.isupper())
    lower = sum(1 for ch in letters if ch.islower())
    ratio_upper = upper / len(letters)
    ratio_lower = lower / len(letters)
    if ratio_upper >= 0.65 or ratio_lower >= 0.65:
        return True
    return bool(re.search(r"[a-z][A-Z]", str(text or "")))


def is_roman_or_acronym_token(token: str) -> bool:
    core = str(token or "").strip()
    if not core:
        return False
    if core.upper() in ROMAN_NUMERALS_KEEP:
        return True
    if core.upper() in ACRONYM_KEEP:
        return True
    if core.isalpha() and core.isupper() and 2 <= len(core) <= 5:
        return True
    return False


def title_case_core_token(token: str, is_first_word: bool) -> str:
    raw = str(token or "")
    if not raw:
        return raw
    if raw.isdigit():
        return raw

    low = raw.lower()
    up = raw.upper()

    if is_roman_or_acronym_token(raw):
        return up
    if (not is_first_word) and low in TITLE_STOPWORDS:
        return low

    if "'" in low:
        parts = [p for p in low.split("'")]
        if len(parts) == 2 and len(parts[1]) <= 2:
            left = parts[0][:1].upper() + parts[0][1:] if parts[0] else ""
            right = parts[1]
            return f"{left}'{right}" if left or right else raw
        out_parts: List[str] = []
        for i, part in enumerate(parts):
            if not part:
                out_parts.append(part)
                continue
            if i > 0 and part in TITLE_STOPWORDS:
                out_parts.append(part)
            else:
                out_parts.append(part[:1].upper() + part[1:])
        return "'".join(out_parts)

    return low[:1].upper() + low[1:]


def apply_word_transform(text: str, transform) -> str:
    out_parts: List[str] = []
    cursor = 0
    word_idx = 0
    for match in WORD_TOKEN_RE.finditer(str(text or "")):
        out_parts.append(text[cursor : match.start()])
        token = match.group(0)
        out_parts.append(transform(token, word_idx == 0))
        cursor = match.end()
        word_idx += 1
    out_parts.append(text[cursor:])
    return "".join(out_parts)


def apply_stopword_lowering_only(text: str) -> str:
    def _transform(token: str, is_first_word: bool) -> str:
        if is_roman_or_acronym_token(token):
            return token.upper()
        if (not is_first_word) and token.lower() in TITLE_STOPWORDS:
            return token.lower()
        return token

    return apply_word_transform(text, _transform)


def smart_title_case(text: str) -> str:
    return apply_word_transform(text, title_case_core_token)


def clean_title_display(title: str) -> str:
    text = normalize_quotes_and_spaces(str(title or ""))
    text = normalize_dash_separator(text)
    if " - " in text:
        text = text.split(" - ", 1)[0].strip()
    if mostly_upper_or_lower(text):
        text = smart_title_case(text)
    else:
        text = apply_stopword_lowering_only(text)
    text = SPACE_RE.sub(" ", text).strip()
    return text


def split_artists_text(text: str) -> List[str]:
    raw = str(text or "").strip()
    if not raw:
        return []
    for sep in ["|", ";", "/", "\\"]:
        raw = raw.replace(sep, ",")
    vals: List[str] = []
    for item in raw.split(","):
        v = str(item).strip()
        if not v:
            continue
        if v not in vals:
            vals.append(v)
    return vals


def parse_artists_canon(value: object) -> List[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return []

    # Para diversidad de artistas, solo admitimos separadores explícitos de lista.
    norm = text.replace(" | ", "|").replace(";", "|")
    parts = [part.strip() for part in norm.split("|")]
    return [part for part in parts if part]


def artists_from_full_json(raw: object) -> List[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    if not (text.startswith("[") and text.endswith("]")):
        return []
    try:
        arr = json.loads(text)
    except Exception:
        return []
    if not isinstance(arr, list):
        return []
    names: List[str] = []
    for row in arr:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if name and name not in names:
            names.append(name)
    return names


def parse_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def build_artists_display(artists: List[str], artists_count: int) -> str:
    names = [str(a).strip() for a in artists if str(a).strip()]
    if not names:
        return ""

    count = int(artists_count) if int(artists_count) > 0 else len(names)
    if count <= 3 or len(names) <= 3:
        return ", ".join(names[: max(1, min(len(names), count))])

    shown = names[:3]
    extra = max(0, count - 3)
    if extra <= 0:
        return ", ".join(shown)
    return f"{', '.join(shown)} +{extra}"


def normalize_basic(text: str) -> str:
    out = str(text or "").lower().strip()
    out = FEAT_BRACKET_RE.sub(" ", out)
    out = FEAT_INLINE_RE.sub(" ", out)
    out = PUNCT_RE.sub(" ", out)
    out = SPACE_RE.sub(" ", out).strip()
    return out


def contains_any_pattern(text: str, patterns: Sequence[str]) -> bool:
    for pattern in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return True
    return False


def remove_version_tags_from_title(text: str) -> str:
    out = str(text or "")
    parts = [p.strip() for p in out.split(" - ")]
    if len(parts) > 1:
        suffix = " ".join(parts[1:])
        if contains_any_pattern(suffix, VERSION_TAG_PATTERNS):
            out = parts[0].strip()

    out = FEAT_BRACKET_RE.sub(" ", out)

    cleaned = out
    for pattern in VERSION_TAG_PATTERNS:
        cleaned = re.sub(r"[\(\[][^\)\]]*" + pattern + r"[^\)\]]*[\)\]]", " ", cleaned, flags=re.IGNORECASE)

    cleaned = FEAT_INLINE_RE.sub(" ", cleaned)
    cleaned = PUNCT_RE.sub(" ", cleaned.lower())
    cleaned = SPACE_RE.sub(" ", cleaned).strip()
    return cleaned


def build_collapse_key(title: str, artists: str) -> str:
    title_raw = str(title or "")
    artists_norm = normalize_basic(artists)
    if contains_any_pattern(title_raw, KEEP_DISTINCT_PATTERNS):
        title_norm = normalize_basic(title_raw)
        variant_prefix = "variant"
    else:
        title_norm = remove_version_tags_from_title(title_raw)
        variant_prefix = "base"

    if not title_norm:
        title_norm = "unknown_title"
    if not artists_norm:
        artists_norm = "unknown_artist"

    return f"{artists_norm}||{variant_prefix}||{title_norm}"


def normalize_album_name(name: str) -> str:
    out = str(name or "").strip().lower()
    out = PUNCT_RE.sub(" ", out)
    out = SPACE_RE.sub(" ", out).strip()
    return out


def build_album_key(album_id: str, album_name: str, canonical_id: str) -> str:
    aid = str(album_id or "").strip()
    if aid:
        return aid

    aname = normalize_album_name(album_name)
    if aname:
        return f"name::{aname}"

    return f"cid::{str(canonical_id or '').strip()}"


def load_manual_title_overrides(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["canonical_id", "title_display", "artists_display"]).to_csv(
            path,
            index=False,
            encoding="utf-8",
        )
        return {}

    df = pd.read_csv(path).fillna("")
    if df.empty:
        return {}

    if "canonical_id" not in df.columns:
        return {}

    if "title_display" not in df.columns:
        df["title_display"] = ""
    if "artists_display" not in df.columns:
        df["artists_display"] = ""

    out: Dict[str, Dict[str, str]] = {}
    for row in df.itertuples(index=False):
        cid = str(getattr(row, "canonical_id", "")).strip()
        if not cid:
            continue
        out[cid] = {
            "title_display": str(getattr(row, "title_display", "")).strip(),
            "artists_display": str(getattr(row, "artists_display", "")).strip(),
        }
    return out


def load_linked_instances(expansion: str, owner_legacy: str, input_linked: Optional[str]) -> pd.DataFrame:
    def read_one(path: Path) -> pd.DataFrame:
        frame = pd.read_csv(path).fillna("")
        frame["_source_file"] = path.name
        return frame

    if input_linked:
        path = Path(input_linked)
        if any(ch in input_linked for ch in ["*", "?"]):
            files = sorted(Path().glob(input_linked))
            if not files:
                raise FileNotFoundError(f"No matches for --input-linked glob: {input_linked}")
            return pd.concat([read_one(f) for f in files], ignore_index=True)

        if path.is_dir():
            files = sorted(path.glob("*.csv"))
            if not files:
                raise FileNotFoundError(f"No CSV files found in --input-linked directory: {path}")
            return pd.concat([read_one(f) for f in files], ignore_index=True)

        if path.is_file():
            return read_one(path)

        raise FileNotFoundError(f"--input-linked not found: {path}")

    pattern = PROCESSED_DIR / f"instances_linked_{expansion}_*.csv"
    files = sorted(pattern.parent.glob(pattern.name))
    if files:
        return pd.concat([read_one(f) for f in files], ignore_index=True)

    legacy = PROCESSED_DIR / f"instances_linked_{expansion}_{owner_legacy}.csv"
    if legacy.exists():
        return read_one(legacy)

    raise FileNotFoundError(
        "No linked instances found. Expected files like "
        f"{pattern} or legacy {legacy}"
    )


def prepare_linked_summary(linked: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    linked = linked.copy().fillna("")

    for col in ["canonical_id", "owner_label", "spotify_url", "spotify_uri", "track_id", "album_id"]:
        if col not in linked.columns:
            linked[col] = ""

    linked["canonical_id"] = linked["canonical_id"].astype(str).str.strip()
    linked = linked[linked["canonical_id"] != ""].copy()

    linked["owner_label"] = linked["owner_label"].astype(str).str.strip()

    linked["album_name_any"] = ""
    for col in ["album_name", "album_name_trim", "album_name_raw"]:
        if col in linked.columns:
            vals = linked[col].astype(str).str.strip()
            linked.loc[linked["album_name_any"].eq("") & vals.ne(""), "album_name_any"] = vals

    agg_map = {
        "spotify_url": ("spotify_url", pick_first_nonempty),
        "spotify_uri": ("spotify_uri", pick_first_nonempty),
        "track_id": ("track_id", pick_first_nonempty),
        "album_id": ("album_id", pick_first_nonempty),
        "album_name_any": ("album_name_any", pick_first_nonempty),
    }
    rep = linked.groupby("canonical_id", as_index=False).agg(**agg_map)
    rep = rep.rename(columns={"album_name_any": "album_name"})

    owners_df = linked.groupby("canonical_id", as_index=False).agg(
        owners_list=(
            "owner_label",
            lambda s: sorted({str(v).strip() for v in s.tolist() if str(v).strip()}, key=lambda x: x.lower()),
        )
    )
    owners_df["owners_count"] = owners_df["owners_list"].apply(len)
    owners_df["owners"] = owners_df["owners_list"].apply(owners_to_display)

    instances_df = linked.groupby("canonical_id", as_index=False).size().rename(columns={"size": "instances_count"})

    summary = rep.merge(owners_df, on="canonical_id", how="outer")
    summary = summary.merge(instances_df, on="canonical_id", how="outer")

    for text_col in ["spotify_url", "spotify_uri", "track_id", "album_id", "album_name", "owners"]:
        if text_col not in summary.columns:
            summary[text_col] = ""
        summary[text_col] = summary[text_col].fillna("").astype(str)

    if "owners_list" not in summary.columns:
        summary["owners_list"] = [[] for _ in range(len(summary))]
    summary["owners_list"] = summary["owners_list"].apply(lambda x: x if isinstance(x, list) else parse_owners_field(x))

    for col in ["owners_count", "instances_count"]:
        if col not in summary.columns:
            summary[col] = 0
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).astype(int)

    owners_universe = sorted({str(v).strip() for v in linked["owner_label"].tolist() if str(v).strip()}, key=lambda s: s.lower())
    return summary, owners_universe


def build_candidates(
    canon: pd.DataFrame,
    linked_summary: pd.DataFrame,
    expansion: str,
    title_overrides: Dict[str, Dict[str, str]],
) -> pd.DataFrame:
    canon = canon.copy().fillna("")
    if "canonical_id" not in canon.columns:
        raise ValueError("canonical input must contain canonical_id")

    canon["canonical_id"] = canon["canonical_id"].astype(str).str.strip()
    canon = canon[canon["canonical_id"] != ""].copy()

    for col in [
        "title_canon",
        "artists_canon",
        "artists_all",
        "artists_count",
        "artists_ids",
        "artists_full_json",
        "year",
        "year_confidence",
        "year_source",
        "year_note",
        "album_id",
        "album_name",
    ]:
        if col not in canon.columns:
            canon[col] = ""

    out = canon.merge(linked_summary, on="canonical_id", how="left")

    for col in [
        "spotify_url",
        "spotify_uri",
        "track_id",
        "album_id",
        "album_name",
        "owners",
        "year_source",
        "year_note",
        "artists_all",
        "artists_ids",
        "artists_full_json",
    ]:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("").astype(str)

    if "owners_list" not in out.columns:
        out["owners_list"] = [[] for _ in range(len(out))]
    out["owners_list"] = out["owners_list"].apply(lambda x: x if isinstance(x, list) else parse_owners_field(x))

    for col in ["owners_count", "instances_count", "artists_count"]:
        if col not in out.columns:
            out[col] = 0
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)

    out["year_confidence"] = out["year_confidence"].apply(parse_float)
    out["year_int"] = out["year"].apply(parse_year_int)

    out["artists_all"] = out["artists_all"].astype(str).str.strip()
    out["artists_canon"] = out["artists_canon"].astype(str).str.strip()
    out.loc[out["artists_all"].eq(""), "artists_all"] = out["artists_canon"]
    out["artists_count"] = out["artists_count"].astype(int)
    for idx, row in out.iterrows():
        if int(row.get("artists_count", 0)) > 0:
            continue
        inferred = len(split_artists_text(row.get("artists_all", "")))
        out.at[idx, "artists_count"] = inferred

    out["title_original"] = out["title_canon"].astype(str).str.strip()
    out["title_display"] = out["title_original"].astype(str).apply(clean_title_display)

    def _artists_display_from_row(row: pd.Series) -> str:
        artists_from_json = artists_from_full_json(row.get("artists_full_json", ""))
        artists_from_all = split_artists_text(row.get("artists_all", ""))
        artists_from_canon = split_artists_text(row.get("artists_canon", ""))
        artists = artists_from_json or artists_from_all or artists_from_canon
        count = parse_int(row.get("artists_count", 0), default=0)
        if count <= 0:
            count = len(artists)
        return build_artists_display(artists, count)

    out["artists_display"] = out.apply(_artists_display_from_row, axis=1)
    out["artists_display"] = out["artists_display"].astype(str).str.strip()
    out.loc[out["artists_display"].eq(""), "artists_display"] = out["artists_canon"].astype(str).str.strip()

    if title_overrides:
        for canonical_id, override in title_overrides.items():
            mask = out["canonical_id"].eq(canonical_id)
            if not mask.any():
                continue
            title_override = str(override.get("title_display", "")).strip()
            artists_override = str(override.get("artists_display", "")).strip()
            if title_override:
                out.loc[mask, "title_display"] = title_override
            if artists_override:
                out.loc[mask, "artists_display"] = artists_override
    out["has_spotify_url"] = out["spotify_url"].astype(str).str.strip().ne("").astype(int)

    out["owners"] = out["owners_list"].apply(owners_to_display)
    out["expansion_code"] = expansion
    out["card_id"] = out["expansion_code"].astype(str) + "-" + out["canonical_id"].astype(str).str.slice(0, 8)

    out["collapse_key"] = out.apply(
        lambda r: build_collapse_key(str(r.get("title_canon", "")), str(r.get("artists_canon", ""))),
        axis=1,
    )
    out["album_key"] = out.apply(
        lambda r: build_album_key(str(r.get("album_id", "")), str(r.get("album_name", "")), str(r.get("canonical_id", ""))),
        axis=1,
    )

    return out


def round_by_mode(value: float, mode: str) -> int:
    if mode == "floor":
        return int(math.floor(value))
    if mode == "ceil":
        return int(math.ceil(value))
    return int(math.floor(value + 0.5))


def build_manual_year_queue(
    candidates: pd.DataFrame,
    owners_universe: List[str],
    expansion: str,
    year_confidence_min: float,
    manual_alpha: float,
    manual_rounding: str,
    manual_min_k: int,
    manual_queue_path: Path,
    reports_prefix: Path,
) -> Tuple[pd.DataFrame, int]:
    owners_n = len(owners_universe)
    k = max(int(manual_min_k), round_by_mode(float(manual_alpha) * float(owners_n), manual_rounding))

    year_invalid = candidates["year_int"].isna() | (candidates["year_confidence"] < float(year_confidence_min))
    queue = candidates[year_invalid & (candidates["owners_count"] >= k)].copy()

    queue["manual_k"] = int(k)
    queue["owners_universe_count"] = int(owners_n)
    queue["year_confidence_min"] = float(year_confidence_min)
    queue["queue_reason"] = "year_missing_or_low_confidence_and_high_owner_presence"

    queue = queue.sort_values(
        ["owners_count", "instances_count", "year_confidence", "canonical_id"],
        ascending=[False, False, True, True],
    )

    queue_cols = [
        "canonical_id",
        "title_canon",
        "artists_canon",
        "owners",
        "owners_count",
        "instances_count",
        "year",
        "year_confidence",
        "year_source",
        "year_note",
        "manual_k",
        "owners_universe_count",
        "year_confidence_min",
        "queue_reason",
    ]
    for col in queue_cols:
        if col not in queue.columns:
            queue[col] = ""
    queue = queue[queue_cols]

    manual_queue_path.parent.mkdir(parents=True, exist_ok=True)
    queue.to_csv(manual_queue_path, index=False, encoding="utf-8")

    queue_report_path = reports_prefix / f"manual_year_queue_{expansion}.csv"
    queue_report_path.parent.mkdir(parents=True, exist_ok=True)
    queue.to_csv(queue_report_path, index=False, encoding="utf-8")

    return queue, int(k)


def collapse_versions(candidates: pd.DataFrame, expansion: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    chosen_rows: List[pd.Series] = []
    report_rows: List[Dict[str, object]] = []

    for collapse_key, group in candidates.groupby("collapse_key", dropna=False):
        ordered = group.sort_values(
            ["owners_count", "instances_count", "has_spotify_url", "canonical_id"],
            ascending=[False, False, False, True],
        )
        chosen = ordered.iloc[0]
        chosen_rows.append(chosen)

        report_rows.append(
            {
                "collapse_key": str(collapse_key),
                "canonical_id_chosen": str(chosen.get("canonical_id", "")),
                "chosen_reason": "owners_count_desc>instances_count_desc>has_spotify_url_desc",
                "candidate_count": int(len(ordered)),
                "candidate_ids": json.dumps(ordered["canonical_id"].astype(str).tolist(), ensure_ascii=False),
                "candidate_titles": json.dumps(ordered["title_canon"].astype(str).tolist(), ensure_ascii=False),
                "candidate_artists": json.dumps(ordered["artists_canon"].astype(str).tolist(), ensure_ascii=False),
                "candidate_owners_count": json.dumps(ordered["owners_count"].astype(int).tolist(), ensure_ascii=False),
                "candidate_instances_count": json.dumps(ordered["instances_count"].astype(int).tolist(), ensure_ascii=False),
                "candidate_has_spotify_url": json.dumps(ordered["has_spotify_url"].astype(int).tolist(), ensure_ascii=False),
            }
        )

    collapsed = pd.DataFrame(chosen_rows).reset_index(drop=True)
    collapse_report = pd.DataFrame(report_rows)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    collapse_path = REPORTS_DIR / f"collapse_{expansion}.csv"
    collapse_report.to_csv(collapse_path, index=False, encoding="utf-8")

    return collapsed, collapse_report


def select_deck(
    pool: pd.DataFrame,
    limit: int,
    max_per_album: int,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    if pool.empty:
        return pool.copy(), {"album_cap_blocks": 0}

    target_limit = int(limit) if int(limit) > 0 else int(len(pool))

    ordered_pool = pool.sort_values(
        ["year_int", "owners_count", "instances_count", "has_spotify_url", "canonical_id"],
        ascending=[True, False, False, False, True],
    )

    year_groups: Dict[int, List[dict]] = {}
    for year_value, group in ordered_pool.groupby("year_int", dropna=True):
        year_groups[int(year_value)] = group.to_dict(orient="records")

    years_sorted = sorted(year_groups.keys())
    year_counts: Dict[int, int] = defaultdict(int)
    album_counts: Dict[str, int] = defaultdict(int)
    artist_counts: Counter[str] = Counter()

    selected_rows: List[dict] = []
    selected_ids: set[str] = set()
    stats = {"album_cap_blocks": 0}

    def can_take(row: dict) -> bool:
        if str(row.get("canonical_id", "")) in selected_ids:
            return False
        if max_per_album <= 0:
            return True
        album_key = str(row.get("album_key", "")).strip() or f"cid::{row.get('canonical_id', '')}"
        return album_counts[album_key] < max_per_album

    def rarity_score(row: dict) -> int:
        artists_list = parse_artists_canon(row.get("artists_canon", ""))
        if not artists_list:
            return 10**9
        return min(artist_counts.get(artist, 0) for artist in artists_list)

    def candidate_rank(row: dict) -> Tuple[int, int, int, int, str]:
        return (
            -int(row.get("owners_count", 0)),
            int(rarity_score(row)),
            -int(row.get("instances_count", 0)),
            -int(row.get("has_spotify_url", 0)),
            str(row.get("canonical_id", "")),
        )

    def prune_unavailable_rows(year: int) -> None:
        rows = year_groups[year]
        keep: List[dict] = []
        for row in rows:
            cid = str(row.get("canonical_id", ""))
            if cid in selected_ids:
                continue
            if max_per_album > 0:
                album_key = str(row.get("album_key", "")).strip() or f"cid::{cid}"
                if album_counts[album_key] >= max_per_album:
                    stats["album_cap_blocks"] += 1
                    continue
            keep.append(row)
        year_groups[year] = keep

    def has_candidate(year: int) -> bool:
        prune_unavailable_rows(year)
        return len(year_groups[year]) > 0

    def pop_next(year: int) -> Optional[dict]:
        prune_unavailable_rows(year)
        rows = year_groups[year]
        if not rows:
            return None
        best_idx = min(range(len(rows)), key=lambda idx: candidate_rank(rows[idx]))
        return rows.pop(best_idx)

    def add_row(row: dict, phase: str) -> None:
        cid = str(row.get("canonical_id", ""))
        if cid in selected_ids:
            return
        if not can_take(row):
            return

        selected_ids.add(cid)
        year_val = int(row.get("year_int"))
        year_counts[year_val] += 1

        album_key = str(row.get("album_key", "")).strip() or f"cid::{cid}"
        if max_per_album > 0:
            album_counts[album_key] += 1

        row_rarity = rarity_score(row)
        for artist in parse_artists_canon(row.get("artists_canon", "")):
            artist_counts[artist] += 1

        out = dict(row)
        out["_artist_rarity_score"] = int(row_rarity)
        out["_selection_phase"] = phase
        out["_selection_order"] = len(selected_rows) + 1
        selected_rows.append(out)

    for year in years_sorted:
        if len(selected_rows) >= target_limit:
            break
        row = pop_next(year)
        if row is not None:
            add_row(row, phase="coverage")

    while len(selected_rows) < target_limit:
        active_years = [year for year in years_sorted if has_candidate(year)]
        if not active_years:
            break

        target_year = min(active_years, key=lambda y: (year_counts[y], y))
        row = pop_next(target_year)
        if row is None:
            continue
        add_row(row, phase="waterfill")

    selected = pd.DataFrame(selected_rows)
    return selected, stats


def write_title_normalization_sample(candidates: pd.DataFrame, expansion: str) -> Path:
    sample_path = REPORTS_DIR / f"title_normalization_sample_{expansion}.csv"
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    if candidates.empty:
        pd.DataFrame(columns=["canonical_id", "title_original", "title_display"]).to_csv(
            sample_path,
            index=False,
            encoding="utf-8",
        )
        return sample_path

    if "title_original" not in candidates.columns:
        candidates = candidates.copy()
        if "title_canon" in candidates.columns:
            candidates["title_original"] = candidates["title_canon"].astype(str)
        else:
            candidates["title_original"] = ""

    diffs = candidates[
        candidates["title_original"].astype(str).str.strip()
        != candidates["title_display"].astype(str).str.strip()
    ][["canonical_id", "title_original", "title_display"]].copy()

    diffs = diffs.sort_values("canonical_id").head(50)
    diffs.to_csv(sample_path, index=False, encoding="utf-8")
    return sample_path


def write_deck_reports(
    valid_pool: pd.DataFrame,
    deck_out: pd.DataFrame,
    reports_prefix: Path,
) -> None:
    reports_prefix.mkdir(parents=True, exist_ok=True)

    year_pool = valid_pool.groupby("year_int", as_index=False).size().rename(columns={"size": "pool_count"})
    year_pool = year_pool.sort_values("year_int")
    year_pool.to_csv(reports_prefix / "year_distribution_pool.csv", index=False, encoding="utf-8")

    year_col = "year_int" if "year_int" in deck_out.columns else "year"
    if deck_out.empty or year_col not in deck_out.columns:
        year_deck = pd.DataFrame(columns=["year_int", "deck_count"])
    else:
        year_deck = deck_out.groupby(year_col, as_index=False).size().rename(columns={"size": "deck_count"})
        year_deck = year_deck.rename(columns={year_col: "year_int"}).sort_values("year_int")
    year_deck.to_csv(reports_prefix / "year_distribution_deck.csv", index=False, encoding="utf-8")

    if deck_out.empty:
        owners_dist = pd.DataFrame(columns=["owners_count", "cards"])
    else:
        owners_dist = deck_out.groupby("owners_count", as_index=False).size().rename(columns={"size": "cards"})
        owners_dist = owners_dist.sort_values("owners_count")
    owners_dist.to_csv(reports_prefix / "owners_count_distribution_deck.csv", index=False, encoding="utf-8")

    if not deck_out.empty:
        deck_out.to_csv(reports_prefix / "deck_selection.csv", index=False, encoding="utf-8")


def compute_top_artists(deck_out: pd.DataFrame, top_n: int = 10) -> List[Tuple[str, int]]:
    counts: Counter[str] = Counter()
    if deck_out.empty or "artists_canon" not in deck_out.columns:
        return []
    for raw in deck_out["artists_canon"].astype(str).tolist():
        for artist in parse_artists_canon(raw):
            counts[artist] += 1
    return counts.most_common(max(0, int(top_n)))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expansion", default="I")
    parser.add_argument("--owner", default="Guille", help="Legacy fallback only")

    parser.add_argument("--input-canonical", default=None)
    parser.add_argument("--input-linked", default=None)
    parser.add_argument("--output-csv", default=None)
    parser.add_argument("--output-json", default=None)

    parser.add_argument("--limit", type=int, default=300)
    parser.add_argument("--max-per-album", type=int, default=3)
    parser.add_argument("--prefer-have-spotify-url", action="store_true")

    parser.add_argument("--year-confidence-min", type=float, default=0.80)
    parser.add_argument("--manual-year-alpha", type=float, default=0.67)
    parser.add_argument("--manual-year-rounding", choices=["round", "floor", "ceil"], default="round")
    parser.add_argument("--manual-year-min-k", type=int, default=2)
    parser.add_argument("--manual-year-queue", default=None)

    parser.add_argument("--owner-cap-percent", type=float, default=0.0)
    parser.add_argument("--owner-cap-cards", type=int, default=0)
    parser.add_argument("--owner-cap-slack", type=float, default=0.12)
    parser.add_argument("--relax-owner-cap-if-needed", action="store_true")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    expansion = args.expansion

    in_canonical = (
        Path(args.input_canonical)
        if args.input_canonical
        else (PROCESSED_DIR / f"canonical_songs_{expansion}_enriched.csv")
    )
    if not in_canonical.exists():
        raise FileNotFoundError(f"canonical input not found: {in_canonical}")

    linked = load_linked_instances(expansion=expansion, owner_legacy=args.owner, input_linked=args.input_linked)
    linked_summary, owners_universe = prepare_linked_summary(linked)

    canon = pd.read_csv(in_canonical).fillna("")
    manual_title_overrides_path = MANUAL_DIR / "manual_title_overrides.csv"
    title_overrides = load_manual_title_overrides(manual_title_overrides_path)
    candidates = build_candidates(
        canon=canon,
        linked_summary=linked_summary,
        expansion=expansion,
        title_overrides=title_overrides,
    )

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    MANUAL_DIR.mkdir(parents=True, exist_ok=True)

    reports_prefix = REPORTS_DIR / f"deck_build_{expansion}"
    reports_prefix.mkdir(parents=True, exist_ok=True)

    manual_queue_path = (
        Path(args.manual_year_queue)
        if args.manual_year_queue
        else (MANUAL_DIR / f"manual_year_queue_{expansion}.csv")
    )

    manual_queue, manual_k = build_manual_year_queue(
        candidates=candidates,
        owners_universe=owners_universe,
        expansion=expansion,
        year_confidence_min=float(args.year_confidence_min),
        manual_alpha=float(args.manual_year_alpha),
        manual_rounding=str(args.manual_year_rounding),
        manual_min_k=int(args.manual_year_min_k),
        manual_queue_path=manual_queue_path,
        reports_prefix=reports_prefix,
    )

    collapsed, collapse_report = collapse_versions(candidates, expansion=expansion)

    valid_pool = collapsed[
        collapsed["year_int"].notna() & (collapsed["year_confidence"] >= float(args.year_confidence_min))
    ].copy()

    valid_pool = valid_pool.sort_values(
        ["year_int", "owners_count", "instances_count", "has_spotify_url", "canonical_id"],
        ascending=[True, False, False, False, True],
    )

    deck_out, selection_stats = select_deck(
        pool=valid_pool,
        limit=int(args.limit),
        max_per_album=int(args.max_per_album),
    )

    if not deck_out.empty:
        deck_out["year"] = deck_out["year_int"].astype(int)
    else:
        deck_out["year"] = pd.Series(dtype=int)

    deck_out_full = deck_out.copy()

    keep_cols = [
        "card_id",
        "expansion_code",
        "canonical_id",
        "title_original",
        "title_display",
        "artists_all",
        "artists_count",
        "artists_ids",
        "artists_display",
        "year_int",
        "year",
        "year_confidence",
        "year_source",
        "year_note",
        "spotify_url",
        "spotify_uri",
        "track_id",
        "owners",
        "owners_count",
        "instances_count",
        "album_id",
        "album_name",
        "album_key",
        "collapse_key",
        "_selection_phase",
        "_selection_order",
    ]
    for col in keep_cols:
        if col not in deck_out.columns:
            deck_out[col] = ""
    deck_out = deck_out[keep_cols]

    out_csv = Path(args.output_csv) if args.output_csv else (PROCESSED_DIR / f"deck_{expansion}.csv")
    out_json = Path(args.output_json) if args.output_json else (PROCESSED_DIR / f"deck_{expansion}.json")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    deck_out.to_csv(out_csv, index=False, encoding="utf-8")
    out_json.write_text(json.dumps(deck_out.to_dict(orient="records"), ensure_ascii=False, indent=2), encoding="utf-8")

    write_deck_reports(valid_pool=valid_pool, deck_out=deck_out, reports_prefix=reports_prefix)
    title_sample_path = write_title_normalization_sample(candidates=candidates, expansion=expansion)

    album_cap_violations = 0
    if not deck_out.empty and int(args.max_per_album) > 0:
        album_counts = deck_out.groupby("album_key").size()
        album_cap_violations = int((album_counts > int(args.max_per_album)).sum())

    qc = {
        "cards": [int(len(deck_out))],
        "limit": [int(args.limit)],
        "pool_valid_after_collapse": [int(len(valid_pool))],
        "pool_total_after_collapse": [int(len(collapsed))],
        "collapse_groups": [int(len(collapse_report))],
        "unique_years_pool": [int(valid_pool["year_int"].nunique()) if not valid_pool.empty else 0],
        "unique_years_in_deck": [int(deck_out["year"].nunique()) if not deck_out.empty else 0],
        "owners_universe_count": [int(len(owners_universe))],
        "manual_year_k": [int(manual_k)],
        "manual_year_queue_size": [int(len(manual_queue))],
        "year_confidence_min": [float(args.year_confidence_min)],
        "max_per_album": [int(args.max_per_album)],
        "album_cap_blocks": [int(selection_stats.get("album_cap_blocks", 0))],
        "album_cap_violations": [int(album_cap_violations)],
        "dup_card_id": [int(deck_out["card_id"].duplicated().sum()) if not deck_out.empty else 0],
        "dup_canonical_id": [int(deck_out["canonical_id"].duplicated().sum()) if not deck_out.empty else 0],
    }

    qc_path = REPORTS_DIR / f"deck_qc_{expansion}.csv"
    pd.DataFrame(qc).to_csv(qc_path, index=False, encoding="utf-8")

    meta = {
        "expansion": expansion,
        "limit": int(args.limit),
        "selected": int(len(deck_out)),
        "owners_universe": owners_universe,
        "manual_year_alpha": float(args.manual_year_alpha),
        "manual_year_rounding": str(args.manual_year_rounding),
        "manual_year_k": int(manual_k),
        "manual_year_queue_size": int(len(manual_queue)),
        "year_confidence_min": float(args.year_confidence_min),
        "max_per_album": int(args.max_per_album),
        "album_cap_blocks": int(selection_stats.get("album_cap_blocks", 0)),
        "album_cap_violations": int(album_cap_violations),
    }
    (reports_prefix / "deck_selection_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if len(manual_queue) > 0:
        print(
            "WARNING: "
            f"manual year queue has {len(manual_queue)} rows "
            f"(k={manual_k}, owners_universe={len(owners_universe)})."
        )

    top_artists = compute_top_artists(deck_out_full, top_n=10)
    if top_artists:
        print("[deck] top_artists_canon_in_deck:")
        for artist, count in top_artists:
            print(f"[deck]   {artist}: {count}")

    print(f"OK manual queue -> {manual_queue_path}")
    print(f"OK collapse report -> {REPORTS_DIR / f'collapse_{expansion}.csv'}")
    print(f"OK title normalization sample -> {title_sample_path}")
    print(f"OK deck -> {out_csv}")
    print(f"OK deck json -> {out_json}")
    print(f"OK qc -> {qc_path}")
    print(f"OK reports -> {reports_prefix}")


if __name__ == "__main__":
    main()

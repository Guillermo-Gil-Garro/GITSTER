import pandas as pd, glob, re
from pathlib import Path

paths = sorted(glob.glob("pipeline/data/processed/instances_linked_I_*.csv"))
if not paths:
    raise SystemExit("No encuentro instances_linked_I_*.csv. Ejecuta canonicalize primero.")

df = pd.concat([pd.read_csv(p).assign(_src=Path(p).name) for p in paths], ignore_index=True).fillna("")

def base_title(s: str) -> str:
    s = str(s).lower().strip()
    s = re.sub(r"\(.*?\)", "", s)  # quita paréntesis
    s = re.sub(r"\b(remaster(ed)?|radio edit|edit|mix|remix|live|acoustic|mono|stereo|version)\b", "", s)
    s = re.sub(r"[-–—]\s*$", "", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def base_artist(s: str) -> str:
    s = str(s).lower().strip()
    s = re.split(r",|&| feat\.| feat | featuring ", s)[0].strip()
    return re.sub(r"\s+", " ", s)

title_col = "title_trim" if "title_trim" in df.columns else ("title_raw" if "title_raw" in df.columns else None)
artist_col = "artists_trim" if "artists_trim" in df.columns else ("artists_raw" if "artists_raw" in df.columns else None)
if not title_col or not artist_col:
    raise SystemExit(f"No encuentro columnas de titulo/artista. cols={list(df.columns)}")

df["base_title"] = df[title_col].map(base_title)
df["base_artist"] = df[artist_col].map(base_artist)

g = df.groupby(["base_title","base_artist"], dropna=False).agg(
    n_rows=("canonical_id","size"),
    n_canonical=("canonical_id", lambda x: x.nunique()),
    canonical_ids=("canonical_id", lambda x: "|".join(sorted(set(map(str,x))))[:500]),
    examples=(title_col, lambda x: " | ".join(sorted(set(map(str,x))) )[:500]),
    artists=(artist_col, lambda x: " | ".join(sorted(set(map(str,x))) )[:500]),
).reset_index()

cand = g[(g["n_canonical"]>=2) & (g["n_rows"]>=2)].sort_values(["n_canonical","n_rows"], ascending=False)
out = Path("pipeline/reports/merge_candidates_I.csv")
out.parent.mkdir(parents=True, exist_ok=True)
cand.to_csv(out, index=False, encoding="utf-8")
print("OK ->", out, "rows:", len(cand))

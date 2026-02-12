import glob
import pandas as pd
import re

canon_path = "pipeline/data/processed/canonical_songs_I_enriched.csv"
canon = pd.read_csv(canon_path, dtype={"year": str}).fillna("")

files = glob.glob("pipeline/data/processed/instances_linked_I_*.csv")
dfs = []
for f in files:
    df = pd.read_csv(f).fillna("")
    if "canonical_id" not in df.columns:
        continue
    cols = [c for c in ["canonical_id","album_id","album_name_trim","album_name_raw"] if c in df.columns]
    dfs.append(df[cols].copy())

print("FILES", len(files), "USED", len(dfs))

inst = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=["canonical_id","album_id","album_name_trim","album_name_raw"])
inst["album_id"] = inst.get("album_id", "").astype(str).str.strip()
inst["album_name_trim"] = inst.get("album_name_trim", "").astype(str).str.strip()

# album_key: prefer album_id; fallback to album_name_trim
inst["album_key"] = inst["album_id"].where(inst["album_id"] != "", inst["album_name_trim"])
inst = inst[inst["album_key"] != ""]

# pick most frequent album_key per canonical_id
counts = (
    inst.groupby(["canonical_id","album_key"], as_index=False)
        .size()
        .sort_values(["canonical_id","size"], ascending=[True, False])
)
top = counts.drop_duplicates("canonical_id", keep="first")

inst2 = inst[["canonical_id","album_key","album_id","album_name_trim"]].drop_duplicates()
top = top.merge(inst2, on=["canonical_id","album_key"], how="left")

canon2 = canon.merge(top[["canonical_id","album_id","album_name_trim"]], on="canonical_id", how="left")
canon2["album_id"] = canon2["album_id"].fillna("")
canon2["album_name"] = canon2["album_name_trim"].fillna("")
canon2 = canon2.drop(columns=["album_name_trim"], errors="ignore")

# year_ok filter
y = canon2["year"].astype(str).str.strip()
ok = y.str.match(r"^\d{4}$")

out = "pipeline/data/processed/canonical_songs_I_year_ok_album.csv"
canon2.loc[ok].to_csv(out, index=False, encoding="utf-8")

album_filled = (canon2.loc[ok, "album_id"].astype(str).str.strip() != "").sum()
album_name_filled = (canon2.loc[ok, "album_name"].astype(str).str.strip() != "").sum()

print("TOTAL", len(canon2), "YEAR_OK", int(ok.sum()), "ALBUM_ID_FILLED", int(album_filled), "ALBUM_NAME_FILLED", int(album_name_filled))
print("WROTE", out)

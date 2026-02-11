import pandas as pd
from pathlib import Path

cand_path = Path("pipeline/reports/merge_candidates_I.csv")
if not cand_path.exists():
    raise SystemExit(f"No existe {cand_path}")

# Preferimos enriched; si no, canonical_songs_I.csv
enriched = Path("pipeline/data/processed/canonical_songs_I_enriched.csv")
canon = Path("pipeline/data/processed/canonical_songs_I.csv")
src_path = enriched if enriched.exists() else canon
if not src_path.exists():
    raise SystemExit("No encuentro canonical_songs_I_enriched.csv ni canonical_songs_I.csv. Ejecuta canonicalize/enrich primero.")

cand = pd.read_csv(cand_path).fillna("")
src = pd.read_csv(src_path).fillna("")

# Explode canonical_ids
rows = []
for _, r in cand.iterrows():
    ids = str(r["canonical_ids"]).split("|")
    for cid in ids:
        rows.append({
            "base_title": r["base_title"],
            "base_artist": r["base_artist"],
            "canonical_id": cid.strip(),
            "group_n_canonical": r["n_canonical"],
            "group_n_rows": r["n_rows"],
            "examples": r["examples"],
            "artists_examples": r["artists"],
        })
x = pd.DataFrame(rows)

# Join con lo que exista en canonical_songs
keep_cols = [c for c in [
    "canonical_id","year","title_display","artists_display",
    "title_trim","artists_trim",
    "track_id","spotify_uri","spotify_url",
    "album_id","album_name","album_release_date"
] if c in src.columns]

y = x.merge(src[keep_cols].drop_duplicates("canonical_id"), on="canonical_id", how="left")

# Señales rápidas para decidir
def has(s, needle):
    return needle in str(s).lower()
y["flag_remix"]   = y["examples"].map(lambda s: has(s,"remix") or has(s," mix"))
y["flag_live"]    = y["examples"].map(lambda s: has(s,"live") or has(s,"sesión") or has(s,"sesion") or has(s,"session"))
y["flag_acoustic"]= y["examples"].map(lambda s: has(s,"acústico") or has(s,"acustico"))
y["flag_feat"]    = y["examples"].map(lambda s: has(s,"feat") or has(s,"with") or has(s,"featuring"))

out = Path("pipeline/reports/merge_review_I.csv")
out.parent.mkdir(parents=True, exist_ok=True)
y.sort_values(["group_n_canonical","group_n_rows","base_artist","base_title"], ascending=False).to_csv(out, index=False, encoding="utf-8")
print("OK ->", out, "rows:", len(y))

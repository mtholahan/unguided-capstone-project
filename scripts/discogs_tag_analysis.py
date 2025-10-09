"""
Discogs metadata quality check.

Analyzes all JSON files saved by discogs_test.py to evaluate
genre/style tagging consistency across releases.
"""

import json
import os
from pathlib import Path
import pandas as pd

# === 1. Define directories ===
data_dir = Path("../data/discogs_raw_v2/soundtrack")  # C:\Projects\unguided-capstone-project\data\discogs_raw_v2\soundtrack
output_csv = Path("../data/discogs_tag_summary.csv")

if not data_dir.exists():
    raise FileNotFoundError("No ../data/discogs_raw/ folder found. Run discogs_test.py first.")

records = []

# === 2. Loop through JSON files ===
for file in data_dir.glob("*.json"):
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    results = data.get("results", [])
    for item in results:
        records.append({
            "movie_json": file.stem,
            "title": item.get("title"),
            "year": item.get("year"),
            "genre": ", ".join(item.get("genre", [])),
            "style": ", ".join(item.get("style", [])),
            "country": item.get("country"),
            "label": ", ".join(item.get("label", [])),
        })

# === 3. Load into DataFrame ===
df = pd.DataFrame(records)
if df.empty:
    print("No Discogs results found. Check your JSONs.")
    exit()

# === 4. Compute tag frequencies ===
def count_keyword(col, keyword):
    return df[col].str.contains(keyword, case=False, na=False).sum()

keywords = ["Soundtrack", "Stage & Screen", "Score", "OST", "Film", "Electronic"]
summary = {kw: count_keyword("genre", kw) + count_keyword("style", kw) for kw in keywords}

summary_df = pd.DataFrame(list(summary.items()), columns=["Tag", "Frequency"]).sort_values("Frequency", ascending=False)

# === 5. Output results ===
print("\n🎬 Discogs Tag Frequency Summary:")
print(summary_df.to_string(index=False))

# Show a few sample rows for inspection
print("\n📄 Sample Titles:")
print(df[["movie_json", "title", "genre", "style", "year"]].head(10).to_string(index=False))

# Save to CSV
summary_df.to_csv(output_csv, index=False)
print(f"\n✅ Tag summary saved to {output_csv}")

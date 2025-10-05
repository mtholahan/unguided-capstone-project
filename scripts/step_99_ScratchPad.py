import pandas as pd
from pathlib import Path

DATA_DIR = Path(r"D:\Capstone_Staging\data\musicbrainz_raw\cleansed")
ac_path = DATA_DIR / "artist_credit.tsv"

print(f"📄 Checking: {ac_path}")

try:
    ac = pd.read_csv(ac_path, sep="\t", header=None, low_memory=False, nrows=10)
    print(f"✅ Loaded {len(ac)} sample rows from artist_credit.tsv")
    print("\n🔎 Sample columns (index-based):")
    print(ac.head(10).to_string(index=False))
except Exception as e:
    print(f"❌ Error reading file: {e}")

import pandas as pd
from pathlib import Path

TMDB_DIR = Path(r"D:\Capstone_Staging\data\tmdb")

print("Step 06 output:", len(pd.read_csv(TMDB_DIR / "enriched_top_1000.csv")))
print("Step 07 output:", len(pd.read_parquet(TMDB_DIR / "tmdb_movies_normalized.parquet")))

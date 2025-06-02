import pandas as pd
from config import DATA_DIR

TMDB_DIR = DATA_DIR / "tmdb"

df = pd.read_csv(TMDB_DIR / "tmdb_match_results.csv")
print(df.columns.tolist())
print(df.head(3))

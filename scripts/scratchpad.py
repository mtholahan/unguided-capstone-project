import pandas as pd
from config import DATA_DIR

IN = DATA_DIR / "tmdb" / "enriched_top_1000.csv"
df = pd.read_csv(IN, nrows=0)
print("Columns in enriched_top_1000.csv:", df.columns.tolist())

import pandas as pd
df = pd.read_parquet("C:/Projects/unguided-capstone-project/data/intermediate/discogs_tmdb_candidates_extended.parquet")
print(df.shape)
print(df.columns)
print(df.head(3))

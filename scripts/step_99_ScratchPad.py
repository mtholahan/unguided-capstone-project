import pandas as pd
df = pd.read_csv(r"D:\Capstone_Staging\data\tmdb\tmdb_input_candidates_clean.csv")
print(df.sample(5)[["title","normalized_title","year"]])

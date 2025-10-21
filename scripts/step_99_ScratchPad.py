import json, pprint
data = json.load(open("data/intermediate/tmdb_raw/Zodiac.json"))
pprint.pp(data.keys())

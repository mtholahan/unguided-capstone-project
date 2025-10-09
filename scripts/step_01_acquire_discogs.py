"""
Discogs API Coverage Test v2

Compares plain vs soundtrack-enhanced queries for TMDb movie titles.
Saves all raw JSON responses and summarizes coverage by query type.
"""

import os
import time
import json
from pathlib import Path
import requests


# === 1. Environment variables from PowerShell ===
DISCOGS_KEY = os.getenv("DISCOGS_CONSUMER_KEY")
DISCOGS_SECRET = os.getenv("DISCOGS_CONSUMER_SECRET")

if not DISCOGS_KEY or not DISCOGS_SECRET:
    raise ValueError("❌ Missing Discogs consumer credentials. Run setup_env.ps1 first!")

HEADERS = {
    "User-Agent": "UnguidedCapstonePipeline/1.1 +http://localhost"
}


# === 2. Test titles (sample from TMDb list) ===
movie_titles = [
    "Star Wars",
    "The Empire Strikes Back",
    "Return of the Jedi",
    "Jurassic Park",
    "E.T. the Extra-Terrestrial",
    "Indiana Jones and the Raiders of the Lost Ark",
    "Jaws",
    "The Lord of the Rings: The Fellowship of the Ring",
    "The Lord of the Rings: The Two Towers",
    "The Lord of the Rings: The Return of the King",
    "Harry Potter and the Sorcerer's Stone",
    "Titanic",
    "Pulp Fiction",
    "The Godfather",
    "The Godfather Part II",
    "The Dark Knight",
    "Gladiator",
    "Inception",
    "Back to the Future",
    "Frozen",
]


# === 3. Output directories ===
base_dir = Path("../data/discogs_raw_v2")
plain_dir = base_dir / "plain"
soundtrack_dir = base_dir / "soundtrack"
for d in [plain_dir, soundtrack_dir]:
    d.mkdir(parents=True, exist_ok=True)


# === 4. Core search function ===
def search_discogs(title, mode="plain", sleep=1.5):
    """Query Discogs for title or 'title soundtrack' and return soundtrack-like results."""
    query = title if mode == "plain" else f"{title} soundtrack"
    url = "https://api.discogs.com/database/search"
    params = {
        "q": query,
        "type": "release",
        "per_page": 5,
        "key": DISCOGS_KEY,
        "secret": DISCOGS_SECRET,
    }

    r = requests.get(url, params=params, headers=HEADERS)
    data = r.json()

    # Save raw response
    out_dir = plain_dir if mode == "plain" else soundtrack_dir
    with open(out_dir / f"{title.replace(' ', '_')}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"📁 Saved {mode} JSON: {title}")

    if r.status_code != 200:
        print(f"⚠️ {title} ({mode}): error {r.status_code} ({r.text})")
        return 0

    results = data.get("results", [])

    # Filter: look for clear soundtrack/score tags
    keywords = ["soundtrack", "score", "stage & screen", "ost", "original motion picture"]
    matches = [
        item for item in results
        if any(
            kw in str((item.get("genre") or []) + (item.get("style") or []) + [item.get("title", "")]).lower()
            for kw in keywords
        )
    ]

    time.sleep(sleep)
    return len(matches)


# === 5. Run comparisons ===
coverage = []
for title in movie_titles:
    plain_hits = search_discogs(title, "plain")
    soundtrack_hits = search_discogs(title, "soundtrack")
    coverage.append({
        "movie": title,
        "plain_hits": plain_hits,
        "soundtrack_hits": soundtrack_hits
    })
    print(f"{title:20} Plain: {plain_hits:<3}  Soundtrack: {soundtrack_hits:<3}")


# === 6. Summary ===
total_titles = len(movie_titles)
plain_total = sum(c["plain_hits"] > 0 for c in coverage)
soundtrack_total = sum(c["soundtrack_hits"] > 0 for c in coverage)

print("\n🎯 Coverage Comparison Summary:")
print(f"Plain query:      {plain_total}/{total_titles} titles ({plain_total/total_titles:.1%})")
print(f"With 'soundtrack': {soundtrack_total}/{total_titles} titles ({soundtrack_total/total_titles:.1%})")

print("\n✅ Raw responses saved to data/discogs_raw_v2/{plain, soundtrack}/")

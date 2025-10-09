"""
config.py v2  —  Unified configuration for Discogs→TMDB pipeline
Author: Mark Holahan
Created: 2025-10-09
Purpose:
    Replaces MusicBrainz-specific constants with Discogs + TMDB equivalents.
    Populates progressively as steps 01–07 are refactored.
"""

from pathlib import Path
import os


# === 1. Core metadata ===
PROJECT_NAME = "UnguidedCapstone_DiscogsPipeline"
VERSION = "0.2-dev"
SOURCE_SYSTEM = "discogs"           # or "tmdb" if context-specific
ENV = os.getenv("ENV", "dev")


# === 2. Directory structure ===
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Discogs
DISCOGS_RAW_DIR = DATA_DIR / "discogs_raw"
DISCOGS_SUMMARY_PATH = DATA_DIR / "discogs_summary.tsv"

# TMDB
TMDB_RAW_DIR = DATA_DIR / "tmdb_raw"
TMDB_OUTPUT_DIR = DATA_DIR / "tmdb_enriched"

# Logs / temp
LOG_DIR = BASE_DIR / "logs"
TMP_DIR = BASE_DIR / "tmp"
for d in [DATA_DIR, DISCOGS_RAW_DIR, TMDB_RAW_DIR, TMDB_OUTPUT_DIR, LOG_DIR, TMP_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# === 3. API performance tuning ===
API_TIMEOUT = 10                # seconds before aborting an API call
RETRY_BACKOFF = 2.0             # seconds between retry attempts (multiplied by attempt count)
MAX_THREADS = 4                 # max concurrent API fetch threads


# === 4. Discogs API ===
DISCOGS_API_URL = "https://api.discogs.com/database/search"
DISCOGS_CONSUMER_KEY = os.getenv("DISCOGS_CONSUMER_KEY", "")
DISCOGS_CONSUMER_SECRET = os.getenv("DISCOGS_CONSUMER_SECRET", "")
DISCOGS_USER_AGENT = "UnguidedCapstonePipeline/1.1 +http://localhost"
DISCOGS_PER_PAGE = 5
DISCOGS_SLEEP_SEC = 1.5
DISCOGS_MAX_RETRIES = 3


# === 5. TMDB API ===
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_API_URL = "https://api.themoviedb.org/3"
TMDB_SEARCH_URL = f"{TMDB_API_URL}/search/movie"
TMDB_DISCOVER_URL = f"{TMDB_API_URL}/discover/movie"
TMDB_GENRE_URL = f"{TMDB_API_URL}/genre/movie/list"
TMDB_MAX_RESULTS = 5
TMDB_SLEEP_SEC = 0.25
# TMDB rate limiting
# Approximate safe limits per API key: 40 requests per 10 seconds (4/sec)
# Set lower (2–3/sec) for stability and to prevent 429 errors.
TMDB_RATE_LIMIT = float(os.getenv("TMDB_RATE_LIMIT", 3.0))


# === 6. Runtime / behavior ===
API_TIMEOUT = 10                # seconds to wait before aborting a request
RETRY_BACKOFF = 2.0             # seconds between retries (multiplied by attempt count)
MAX_THREADS = 4                 # for future parallel fetches
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
SAVE_RAW_JSON = True            # toggle file writes for large batch runs
GOLDEN_TEST_MODE = True         # 


# === 7. Schema / normalization ===
DISCOGS_FIELDS = [
    "title", "year", "genre", "style", "country", "label", "id", "uri"
]
TMDB_FIELDS = [
    "id", "title", "release_date", "genres", "popularity", "vote_average"
]
JOIN_KEYS = ["title", "year"]  # to be validated in step_04_match_instrumented


# === 8. Utility ===
def print_config_summary():
    print(f"[Config] Environment: {ENV}")
    print(f"[Config] Data dir: {DATA_DIR}")
    print(f"[Config] Source: {SOURCE_SYSTEM}")
    print(f"[Config] Discogs raw → {DISCOGS_RAW_DIR}")
    print(f"[Config] TMDB output → {TMDB_OUTPUT_DIR}")
    print(f"[Config] Log level: {LOG_LEVEL}")

# === Golden Movie List ===
GOLDEN_TITLES = [
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

GOLDEN_EXPECTED_YEARS = {
    "Star Wars": 1977,
    "The Empire Strikes Back": 1980,
    "Return of the Jedi": 1983,
    "Jurassic Park": 1993,
    "E.T. the Extra-Terrestrial": 1982,
    "Indiana Jones and the Raiders of the Lost Ark": 1981,
    "Jaws": 1975,
    "The Lord of the Rings: The Fellowship of the Ring": 2001,
    "The Lord of the Rings: The Two Towers": 2002,
    "The Lord of the Rings: The Return of the King": 2003,
    "Harry Potter and the Sorcerer's Stone": 2001,
    "Titanic": 1997,
    "Pulp Fiction": 1994,
    "The Godfather": 1972,
    "The Godfather Part II": 1974,
    "The Dark Knight": 2008,
    "Gladiator": 2000,
    "Inception": 2010,
    "Back to the Future": 1985,
    "Frozen": 2013,
}

GOLDEN_TITLES_TEST = GOLDEN_TITLES[:5]  # First 5 for quick local runs

def get_active_title_list():
    """Return the appropriate movie list based on ENV."""
    if ENV.lower() in ("dev", "local"):
        print("[Config] Using TEST title subset (5 titles).")
        return GOLDEN_TITLES_TEST
    print("[Config] Using FULL Golden Title list.")
    return GOLDEN_TITLES

if __name__ == "__main__":
    print_config_summary()

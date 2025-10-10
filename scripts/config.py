"""
config.py v3.0 — Unified configuration for Discogs→TMDB pipeline
Author: Mark Holahan
Refactor Date: 2025-10-09

Purpose:
    Centralized configuration and constants for the Unguided Capstone project.
    Defines all paths, API settings, concurrency limits, and runtime behavior.
    Designed for both local and cloud-safe operation.
"""

import os
from pathlib import Path
import multiprocessing

# ===============================================================
# 🧭 PROJECT METADATA
# ---------------------------------------------------------------
# Core identifiers used for logging, versioning, and environment awareness.
# ===============================================================
PROJECT_NAME = "UnguidedCapstone_DiscogsPipeline"
VERSION = "0.3-prod"
SOURCE_SYSTEM = "discogs"  # Default data origin for initial pipeline stages
ENV = os.getenv("ENV", "dev")  # "dev", "local", "prod" — affects test data scope


# ===============================================================
# 🧱 DIRECTORY STRUCTURE
# ---------------------------------------------------------------
# Defines all working directories for raw data, intermediate artifacts,
# metrics, logs, and temporary files. Created automatically at runtime.
# ===============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
METRICS_DIR = DATA_DIR / "metrics"
LOG_DIR = BASE_DIR / "logs"
TMP_DIR = BASE_DIR / "tmp"

# Specific subdirectories (Discogs, TMDB)
DISCOGS_RAW_DIR = RAW_DIR / "discogs_raw"
TMDB_RAW_DIR = RAW_DIR / "tmdb_raw"
TMDB_OUTPUT_DIR = DATA_DIR / "tmdb_enriched"

# Ensure all required folders exist
for d in [DATA_DIR, RAW_DIR, INTERMEDIATE_DIR, METRICS_DIR, LOG_DIR, TMP_DIR,
          DISCOGS_RAW_DIR, TMDB_RAW_DIR, TMDB_OUTPUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ===============================================================
# ⚙️ CONCURRENCY SETTINGS
# ---------------------------------------------------------------
# Controls parallel execution limits per API; scales based on CPU count.
# ===============================================================
CPU_CORES = multiprocessing.cpu_count()

DISCOGS_MAX_WORKERS = int(os.getenv("DISCOGS_MAX_WORKERS", min(4, CPU_CORES)))
TMDB_MAX_WORKERS = int(os.getenv("TMDB_MAX_WORKERS", min(6, CPU_CORES)))
DEFAULT_MAX_WORKERS = int(os.getenv("DEFAULT_MAX_WORKERS", min(4, CPU_CORES)))
# Global thread limit — hard ceiling for any thread pool
MAX_THREADS = int(os.getenv("MAX_THREADS", min(8, CPU_CORES * 2)))


def get_safe_workers(api_name: str) -> int:
    """Return safe thread count per API, capped by CPU cores."""
    mapping = {
        "discogs": DISCOGS_MAX_WORKERS,
        "tmdb": TMDB_MAX_WORKERS,
        "default": DEFAULT_MAX_WORKERS,
    }
    workers = mapping.get(api_name.lower(), DEFAULT_MAX_WORKERS)
    return max(1, min(workers, CPU_CORES))


# ===============================================================
# 🌐 API PERFORMANCE TUNING
# ---------------------------------------------------------------
# Shared timeout, retry, and backoff parameters for API calls.
# ===============================================================
API_TIMEOUT = 10.0               # Seconds before an API request aborts
RETRY_BACKOFF = 2.0              # Delay between retries (exponential multiplier)
SAVE_RAW_JSON = True             # Whether to write raw API responses to disk
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")  # Default verbosity for loggers


# ===============================================================
# 🎵 DISCOGS API SETTINGS
# ---------------------------------------------------------------
# Base URL, keys, and pagination controls for Discogs data acquisition.
# ===============================================================
DISCOGS_API_URL = "https://api.discogs.com/database/search"
DISCOGS_CONSUMER_KEY = os.getenv("DISCOGS_CONSUMER_KEY", "")
DISCOGS_CONSUMER_SECRET = os.getenv("DISCOGS_CONSUMER_SECRET", "")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "UnguidedCapstoneBot/1.0")

DISCOGS_PER_PAGE = 5             # Number of results per request
DISCOGS_SLEEP_SEC = 1.5          # Throttle delay between requests
DISCOGS_MAX_RETRIES = 3          # Retry attempts before giving up

MAX_DISCOG_TITLES = 200          # Adjustable batch size


# ===============================================================
# 🎬 TMDB API SETTINGS
# ---------------------------------------------------------------
# Base URL and endpoints for TMDB movie data fetching and discovery.
# ===============================================================
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_API_URL = "https://api.themoviedb.org/3"
TMDB_SEARCH_URL = f"{TMDB_API_URL}/search/movie"
TMDB_DISCOVER_URL = f"{TMDB_API_URL}/discover/movie"
TMDB_GENRE_URL = f"{TMDB_API_URL}/genre/movie/list"

TMDB_MAX_RESULTS = 5
TMDB_SLEEP_SEC = 0.25            # Delay between calls for rate stability

# Rate limiter target — max requests per second
TMDB_RATE_LIMIT = float(os.getenv("TMDB_RATE_LIMIT", 3.0))


# ===============================================================
# 🔢 FUZZY MATCHING PARAMETERS
# ---------------------------------------------------------------
# Used by Step 04 (Discogs↔TMDB join) to control matching behavior.
# ===============================================================
FUZZ_THRESHOLD = int(os.getenv("FUZZ_THRESHOLD", 85))   # Minimum score (0–100)
YEAR_VARIANCE = int(os.getenv("YEAR_VARIANCE", 3))      # Allowable year gap
TOP_N = int(os.getenv("TOP_N", 5))                      # Top candidates per match


# ===============================================================
# ☁️ AZURE PLACEHOLDER
# ---------------------------------------------------------------
# Placeholder for optional future blob storage integration.
# ===============================================================
AZURE_SAS_TOKEN = os.getenv("AZURE_SAS_TOKEN", None)


# ===============================================================
# 🧮 SCHEMA & NORMALIZATION SETTINGS
# ---------------------------------------------------------------
# Defines canonical field names and join keys for harmonization.
# ===============================================================
DISCOGS_FIELDS = [
    "title", "year", "genre", "style", "country", "label", "id", "uri"
]
TMDB_FIELDS = [
    "id", "title", "release_date", "genres", "popularity", "vote_average"
]
JOIN_KEYS = ["title", "year"]  # Default harmonization join columns


# ===============================================================
# 🎬 GOLDEN TEST DATASETS
# ---------------------------------------------------------------
# Reference title lists for pipeline validation and quick local testing.
# ===============================================================

USE_GOLDEN_LIST = False     # A toggle to swith to using below hard-coded list (or subset)
                            # or using real-time pull against Discogs API
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

GOLDEN_TITLES_TEST = GOLDEN_TITLES[:5]  # First 5 for lightweight dev runs


def get_active_title_list():
    """Return either the test or full title set based on environment."""
    if ENV.lower() in ("dev", "local"):
        print("[Config] Using TEST title subset (5 titles).")
        return GOLDEN_TITLES_TEST
    print("[Config] Using FULL Golden Title list.")
    return GOLDEN_TITLES


# ===============================================================
# 🧾 UTILITY FUNCTIONS
# ===============================================================
def print_config_summary():
    """Print a short summary of key configuration paths."""
    print(f"[Config] Environment: {ENV}")
    print(f"[Config] Data directory: {DATA_DIR}")
    print(f"[Config] Raw data path: {RAW_DIR}")
    print(f"[Config] Intermediate: {INTERMEDIATE_DIR}")
    print(f"[Config] Discogs raw: {DISCOGS_RAW_DIR}")
    print(f"[Config] TMDB raw: {TMDB_RAW_DIR}")
    print(f"[Config] TMDB output: {TMDB_OUTPUT_DIR}")
    print(f"[Config] Metrics directory: {METRICS_DIR}")
    print(f"[Config] Log level: {LOG_LEVEL}")


# Run summary when executed directly
if __name__ == "__main__":
    print_config_summary()

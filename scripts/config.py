"""
Capstone Project Configuration File
Author: Mark
Last Updated: Mon, 06-Oct-2025

Centralized configuration for the Unguided Capstone ETL pipeline.
Organized into logical sections for readability and maintainability.
"""

# ============================================================
# 1. Imports & Environment Setup
# ============================================================

import os
from pathlib import Path
from dotenv import load_dotenv
import pprint

# Load environment variables
load_dotenv()

def print_config_summary():
    """Pretty-print all uppercase constants for quick debugging."""
    pprint.pp({k: v for k, v in globals().items() if k.isupper()})

# ============================================================
# 2. Core Paths
# ============================================================

BASE_DIR = Path(r"D:/Capstone_Staging")
DATA_DIR = BASE_DIR / "data"
RESULTS_DIR = DATA_DIR / "results"
METRICS_DIR = DATA_DIR / "metrics"

MB_RAW_DIR = DATA_DIR / "musicbrainz_raw"
MB_CLEANSED_DIR = MB_RAW_DIR / "cleansed"
TMDB_DIR = DATA_DIR / "tmdb"

SCRIPTS_PATH = Path(r"C:/Projects/unguided-capstone-project/scripts")
SEVEN_ZIP_PATH = Path(r"C:/Program Files/7-Zip/7z.exe")

# ============================================================
# 3. Database Settings
# ============================================================

PG_HOST = "localhost"
PG_PORT = "5432"
PG_DBNAME = "musicbrainz"
PG_USER = "postgres"
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SCHEMA = "public"

# ============================================================
# 4. Performance, Toggles & Thresholds
# ============================================================

DEBUG_MODE = True
UNATTENDED = True

# ---------------------------------------------------------------------
# 🧮 PHASE 1 – PIPELINE RUNTIME CONSTANTS
# ---------------------------------------------------------------------
# These constants replace inline "magic numbers" in Steps 00–06.
# Each group aligns with a specific part of the ETL pipeline.

# ---------------------------
#  Core Retry / General Loop
# ---------------------------
DEFAULT_RETRY_COUNT: int = 2          # Generic retry limit for small loops
RETRY_DELAY_SECONDS: int = 5          # Delay (seconds) between lightweight retries

# ---------------------------
#  MusicBrainz Acquisition
# ---------------------------
DOWNLOAD_BUFFER_SIZE: int = 1024      # Stream buffer size (bytes) for MusicBrainz dump downloads
MAX_RETRY_ATTEMPTS: int = 10          # Max retries for failed MB downloads

# ---------------------------
#  Audit / Cleansing Phases
# ---------------------------
AUDIT_SAMPLE_LIMIT: int = 30          # Sample size for audit reports
CLEANSE_SAMPLE_LIMIT: int = 20        # Sample size for cleansing previews

# ---------------------------
#  GUID Rehydration / Joins
# ---------------------------
GUID_SAMPLE_LIMIT: int = 6            # Sample size for rehydration preview
GUID_RETRY_LIMIT: int = 40            # Max missing GUIDs tolerated before halt
JOIN_SAMPLE_LIMIT: int = 100          # Row limit for join audits
JOIN_ITERATION_LIMIT: int = 5         # Max join retry attempts
JOIN_TOLERANCE: float = 0.001         # Float precision tolerance for join scoring

# ---------------------------
#  Filtering / Soundtrack Refinement
# ---------------------------
FILTER_THRESHOLD: float = 0.02        # Minimum match score for soundtrack inclusion
FILTER_SAMPLE_SIZE: int = 42          # Debug sample size for diagnostic subsets
SOUNDTRACK_SUBSET_LIMIT: int = 100    # Max rows for subset parquet exports

# ---------------------------
#  TMDb API / data Fetch
# ---------------------------
TMDB_RESULT_LIMIT: int = 1000         # Total titles to query (cap)
TMDB_PAGE_SIZE: int = 500             # Max results per page
TMDB_RETRY_DELAY: int = 20            # Delay (seconds) between TMDb API calls
TMDB_TOTAL_LIMIT: int = 10000         # Hard stop for cumulative fetches


# Performance limits
CHUNK_SIZE = 5_000              # Default CSV / ETL batch size
ROW_LIMIT = 10_000              # Max rows to process per batch
AUDIT_SAMPLE_LIMIT = 100_000    # Sampling limit for audits
SLEEP_SECONDS = 1               # Default throttle for API calls

# Matching & Fuzzy Logic
FUZZY_THRESHOLD = 120           # Default match cutoff
YEAR_TOLERANCE = 1              # Allow ±1 year drift
MAX_CANDIDATES_PER_TITLE = 25   # Candidate cap per title

# Golden test settings
GOLDEN_TEST_MODE = False
GOLDEN_TEST_SIZE = 200

# Metrics dictionary populated dynamically
STEP_METRICS = {}

# ============================================================
# 5. External Services (URLs, Keys, Azure)
# ============================================================

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# URLs
MB_DUMP_URL = "https://data.metabrainz.org/pub/musicbrainz/data/fullexport/"
TMDB_DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"
TMDB_SEARCH_URL = "https://api.themoviedb.org/3/search/movie"
TMDB_GENRE_URL = "https://api.themoviedb.org/3/genre/movie/list"

# Azure storage placeholders
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
BLOB_CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "capstone-outputs")

# ============================================================
# 6. Data File Mappings (MusicBrainz & TMDb)
# ============================================================

# --- MusicBrainz Inputs ---
TSV_WHITELIST = {
    "artist.tsv",
    "artist_credit.tsv",
    "release.tsv",
    "release_group.tsv",
    "release_group_secondary_type.tsv",
    "release_group_secondary_type_join.tsv",
}

MB_RAW_FILES = {name: MB_RAW_DIR / name for name in TSV_WHITELIST}
MB_TSV_FILES = {name: MB_CLEANSED_DIR / name for name in TSV_WHITELIST}
MB_DUMP_ARCHIVE = BASE_DIR / "mbdump.tar.bz2"
MB_DUMP_DIR = BASE_DIR / "mbdump"
MB_PARQUET_SOUNDTRACKS = MB_RAW_DIR / "soundtracks.parquet"
MB_SECONDARY_TYPE_JOIN_FILE = MB_RAW_DIR / "release_group_secondary_type_join_clean.tsv"

# --- MusicBrainz Derived Files ---
MB_RELEASE_ENRICHED_GUIDED_FILE = MB_CLEANSED_DIR / "release_enriched_guided.tsv"
MB_RELEASE_ENRICHED_FILE = MB_CLEANSED_DIR / "release_enriched.tsv"
MB_JOINED_RELEASE_FILE = MB_CLEANSED_DIR / "joined_release_data.tsv"
MB_SOUNDTRACKS_FILE = MB_CLEANSED_DIR / "soundtracks.tsv"
MB_SOUNDTRACKS_PARQUET = MB_CLEANSED_DIR / "soundtracks.parquet"

# --- TMDb Data Inputs ---
TOP_MOVIES_FILE = DATA_DIR / "top_movies_raw.csv"
ENRICHED_FILE = TMDB_DIR / "enriched_top_1000.csv"
TMDB_GENRE_FILE = TMDB_DIR / "tmdb_genre_top_1000.csv"
TMDB_MOVIE_GENRE_FILE = TMDB_DIR / "tmdb_movie_genre_top_1000.csv"
JUNK_TITLES_FILE = DATA_DIR / "junk_title_list.txt"

# --- Output Files ---
MATCHED_TSV = RESULTS_DIR / "matched_top_1000.tsv"
UNMATCHED_TSV = RESULTS_DIR / "unmatched_top_1000.tsv"

diagnostic_files = sorted(
    RESULTS_DIR.glob("matched_diagnostics_1000_*.tsv"),
    key=os.path.getmtime,
    reverse=True,
)
MATCHED_DIAGNOSTICS_TSV = diagnostic_files[0] if diagnostic_files else None

MATCH_OUTPUTS = {
    "matched": MATCHED_TSV,
    "unmatched": UNMATCHED_TSV,
    "diagnostics": MATCHED_DIAGNOSTICS_TSV,
}

REFRESH_INPUTS = {
    "tmdb_movie": str(ENRICHED_FILE),
    "tmdb_genre": str(TMDB_GENRE_FILE),
    "tmdb_movie_genre": str(TMDB_MOVIE_GENRE_FILE),
    "soundtracks": str(MB_PARQUET_SOUNDTRACKS),
    "matched_top_1000": str(MATCHED_TSV),
    "unmatched_top_1000": str(UNMATCHED_TSV),
    "matched_diagnostics": str(MATCHED_DIAGNOSTICS_TSV),
}

MB_STATIC_REFRESH = {name: str(MB_TSV_FILES[name]) for name in TSV_WHITELIST}

# ============================================================
# 7. Reference Data (Golden Titles)
# ============================================================

GOLDEN_TITLES = {
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
}

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

# ============================================================
# 8. Config Class
# ============================================================

class Config:
    """Object-based wrapper for key pipeline paths and settings."""

    def __init__(self):
        self.BASE_DIR = BASE_DIR
        self.DATA_DIR = DATA_DIR
        self.RESULTS_DIR = RESULTS_DIR
        self.MB_RAW_DIR = MB_RAW_DIR
        self.MB_CLEANSED_DIR = MB_CLEANSED_DIR
        self.TMDB_DIR = TMDB_DIR

        self.ROW_LIMIT = ROW_LIMIT
        self.DEBUG_MODE = DEBUG_MODE
        self.UNATTENDED = UNATTENDED

        self.TMDB_API_KEY = TMDB_API_KEY
        self.PG_HOST = PG_HOST
        self.PG_PORT = PG_PORT
        self.PG_DBNAME = PG_DBNAME
        self.PG_USER = PG_USER
        self.PG_PASSWORD = PG_PASSWORD
        self.PG_SCHEMA = PG_SCHEMA

    def __repr__(self):
        return f"<Config DATA_DIR={self.DATA_DIR} ROW_LIMIT={self.ROW_LIMIT}>"
    
    def get(self, key, default=None):
        return getattr(self, key, default)


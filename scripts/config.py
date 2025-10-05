"""
Capstone Project Configuration File
Author: Mark
Last Updated: Thu, 03-Oct-2025

Defines constants for file paths, filenames, and database settings used throughout the ETL pipeline.
Organized by logical grouping: paths, MusicBrainz data, TMDb data, output files, and Postgres.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables (used for DB password)
load_dotenv()

# === PostgreSQL Database Settings ===
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DBNAME = "musicbrainz"
PG_USER = "postgres"
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SCHEMA = "public"

# === Base Paths ===
BASE_DIR = Path("D:/Capstone_Staging")
DATA_DIR = BASE_DIR / "data"
RESULTS_DIR = DATA_DIR / "results"
TMDB_DIR = DATA_DIR / "tmdb"
ENRICHED_TOP1000 = TMDB_DIR / "enriched_top_1000.csv"
MB_RAW_DIR = DATA_DIR / "musicbrainz_raw"
MB_CLEANSED_DIR = MB_RAW_DIR / "cleansed"
SEVEN_ZIP_PATH = Path("C:/Program Files/7-Zip/7z.exe")

# Global toggle for extra debug logging across steps
DEBUG_MODE = True

#=== Whitelist of MusicBrainz TSV Files ===
TSV_WHITELIST = {
    "artist",
    "artist_credit",
    "release",
    "release_group",
    "release_group_secondary_type",
    "release_group_secondary_type_join",
}


# === MusicBrainz File Mappings ===
MB_RAW_FILES = {name: MB_RAW_DIR / name for name in TSV_WHITELIST}
MB_TSV_FILES = {name: MB_CLEANSED_DIR / name for name in TSV_WHITELIST}

# === MusicBrainz Archive Inputs ===
MB_DUMP_ARCHIVE = BASE_DIR / "mbdump.tar.bz2"
MB_DUMP_DIR = BASE_DIR / "mbdump"
MB_PARQUET_SOUNDTRACKS = MB_RAW_DIR / "soundtracks.parquet"
MB_SECONDARY_TYPE_JOIN_FILE = MB_RAW_DIR / "release_group_secondary_type_join_clean.tsv"

# === TMDB API Key ===
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

# === TMDb Data Inputs ===
TOP_MOVIES_FILE = DATA_DIR / "top_movies_raw.csv"
ENRICHED_FILE = TMDB_DIR / "enriched_top_1000.csv"
TMDB_GENRE_FILE = TMDB_DIR / "tmdb_genre_top_1000.csv"
TMDB_MOVIE_GENRE_FILE = TMDB_DIR / "tmdb_movie_genre_top_1000.csv"
JUNK_TITLES_FILE = DATA_DIR / "junk_title_list.txt"

# === Match Output Files ===
MATCHED_TSV = RESULTS_DIR / "matched_top_1000.tsv"
UNMATCHED_TSV = RESULTS_DIR / "unmatched_top_1000.tsv"
diagnostic_files = sorted(RESULTS_DIR.glob("matched_diagnostics_1000_*.tsv"), key=os.path.getmtime, reverse=True)
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

MB_STATIC_REFRESH = {
    name: str(MB_TSV_FILES[name]) for name in TSV_WHITELIST
}

# TMDb Match
YEAR_VARIANCE = 10


# === Global Toggles ===
UNATTENDED = True

# === Testing Toggles ===
# =======================
ROW_LIMIT = 1_000_000

# === Golden Test Mode ===
GOLDEN_TEST_SIZE = 200

GOLDEN_TEST_MODE = True  # Global toggle for golden benchmark runs

# Blockbuster sanity list (Step 06 + Step 08 will both use this)
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

# Canonical release years for disambiguation in Step 06
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

# Global metrics (populated by steps like Step 08)
STEP_METRICS = {}

# Fuzzy-matching parameters (used in Steps 07–08)
FUZZY_THRESHOLD = 120        # default match cutoff
YEAR_TOLERANCE = 1           # allow ±1 year drift
MAX_CANDIDATES_PER_TITLE = 25

"""
config.py
---------------------------------------------------------------
Central configuration for the Discogs→TMDB Data Pipeline
(Springboard Unguided Capstone Project)

Version:
    v3.2 — Oct 2025
Purpose:
    - Restore all previously used constants
    - Unify "mode control" logic (golden vs. active, local vs. API)
    - Maintain backward compatibility for Steps 01–07
---------------------------------------------------------------
"""

import os
import multiprocessing
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv, find_dotenv
import logging

# --- Load .env with override to ensure it wins over global/system env ---
dotenv_path = find_dotenv(usecwd=True)
load_dotenv(dotenv_path, override=True)

# ===============================================================
# 🌎 ENVIRONMENT SETTINGS
# ===============================================================
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
INTERMEDIATE_DIR = DATA_DIR / "intermediate"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = ROOT_DIR / "logs"
METRICS_DIR = DATA_DIR / "metrics"
CPU_CORES = multiprocessing.cpu_count()

ENV = os.getenv("ENV", "dev")  # "dev", "test", or "prod"

for d in [DATA_DIR, RAW_DIR, INTERMEDIATE_DIR, PROCESSED_DIR, LOG_DIR, METRICS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ===============================================================
# 🎛️ PIPELINE MODE CONTROLS
# ===============================================================
USE_GOLDEN_LIST = False          # True → use curated GOLDEN_TITLES
TITLE_LIST_PATH = DATA_DIR / "movie_titles_200.txt"  # Full active title list

RUN_LOCAL = False                # True → offline mode; skip API calls
FORCE_CACHE_ONLY = RUN_LOCAL
SAVE_RAW_JSON = True
ALLOW_API_FETCH = not RUN_LOCAL

DISCOG_MAX_TITLES = 50          # Batch limiter for Step 01
TMDB_MAX_RESULTS = 5
MAX_THREADS = int(os.getenv("MAX_THREADS", min(8, CPU_CORES * 2)))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ===============================================================
# 🕐 API TIMEOUTS / RETRIES
# ===============================================================
API_TIMEOUT = 20                 # seconds
API_MAX_RETRIES = 3
RETRY_BACKOFF = 2.0              # seconds between retries
TMDB_REQUEST_DELAY_SEC = 0.8     # polite pause between TMDB API calls

# ===============================================================
# 🎞️ DISCOGS SETTINGS
# ===============================================================
DISCOGS_API_URL = "https://api.discogs.com/database/search"
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN", "")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "UnguidedCapstoneBot/1.0")

DISCOGS_RAW_DIR = RAW_DIR / "discogs_raw"
DISCOGS_RAW_DIR.mkdir(parents=True, exist_ok=True)

DISCOGS_MAX_RETRIES = 3
DISCOGS_PER_PAGE = 5
DISCOGS_SLEEP_SEC = 3.0     # routine delay between individual API requests
RATE_LIMIT_SLEEP_SEC = 60   # Cooldown period after Discogs returns HTTP 429 (“Too Many Requests”)

# ===============================================================
# 🎥 TMDB SETTINGS
# ===============================================================
TMDB_API_URL = "https://api.themoviedb.org/3/search/movie"
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_SLEEP_SEC = TMDB_REQUEST_DELAY_SEC # deprecated alias for backward compatibility
TMDB_RAW_DIR = RAW_DIR / "tmdb_raw"
TMDB_RAW_DIR.mkdir(parents=True, exist_ok=True)


# ===============================================================
# 🧩 STEP-SPECIFIC PARAMETERS (Backward-Compatible)
# ===============================================================
# These were previously scattered in step scripts; kept here to
# preserve compatibility for Steps 02–04 without breaking imports.

# --- Step 02: TMDB Fetch ---
TMDB_SEARCH_URL = TMDB_API_URL
TMDB_RATE_LIMIT = 40             # max requests per 10 seconds (API guideline)

# --- Step 03: Prepare TMDB Input ---
DEFAULT_MAX_WORKERS = MAX_THREADS   # alias for concurrency defaults

# --- Step 04: Match Discogs ↔ TMDB ---
FUZZ_THRESHOLD = 85             # minimum fuzzy-match ratio for candidate acceptance
YEAR_VARIANCE = 2               # acceptable difference between Discogs/TMDB year
TOP_N = 5                       # number of top TMDB results to consider per title

# ===============================================================
# 🎬 GOLDEN TITLE LISTS
# ===============================================================
GOLDEN_TITLES = [
    "Inception", "Interstellar", "The Dark Knight", "Blade Runner", "The Matrix",
    "Pulp Fiction", "Forrest Gump", "The Godfather", "The Shawshank Redemption", "Fight Club",
    "Back to the Future", "Gladiator", "Titanic", "Avatar", "Jurassic Park",
    "Star Wars", "The Lord of the Rings", "Harry Potter", "La La Land", "The Lion King", "Frozen"
    "Jaws"
]
GOLDEN_TITLES_TEST = GOLDEN_TITLES[:5]

# ===============================================================
# 🎬 TITLE SOURCE RESOLVER
# ===============================================================
def get_active_title_list(path=None):
    """
    Resolve the working title list based on control flags and environment.

    Priority:
      1️⃣ USE_GOLDEN_LIST=True        → GOLDEN_TITLES
      2️⃣ TITLE_LIST_PATH exists      → load from .csv or .txt
      3️⃣ ENV in ('dev','local')      → GOLDEN_TITLES_TEST (5 titles)
      4️⃣ Otherwise                   → raise FileNotFoundError

    Behavior:
      • Reads first column of CSV or each line of TXT.
      • Trims whitespace and drops empty rows.
      • Prints clear message describing which source was used.
    """
    import pandas as pd
    from pathlib import Path

    # 1️⃣ Curated list override
    if USE_GOLDEN_LIST:
        print("[Config] Using curated GOLDEN_TITLES list (USE_GOLDEN_LIST=True).")
        return GOLDEN_TITLES

    # 2️⃣ External file source
    file_path = Path(path or TITLE_LIST_PATH)
    if file_path.exists():
        try:
            if file_path.suffix.lower() == ".csv":
                df = pd.read_csv(file_path)
                titles = df.iloc[:, 0].dropna().astype(str).tolist()
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    titles = [line.strip() for line in f if line.strip()]

            print(f"[Config] Loaded {len(titles)} active titles from {file_path.name}.")
            return titles

        except Exception as e:
            print(f"[Config] ⚠️ Failed to read {file_path}: {e}")

    # 3️⃣ Development fallback
    if ENV.lower() in ("dev", "local"):
        print("[Config] ⚠️ No external title list found — using GOLDEN_TITLES_TEST (dev fallback).")
        return GOLDEN_TITLES_TEST

    # 4️⃣ Production enforcement
    raise FileNotFoundError(
        f"❌ Title list file not found: {file_path}. "
        f"Create this file or set USE_GOLDEN_LIST=True."
    )


# ===============================================================
# 🧮 WORKER MANAGEMENT
# ===============================================================
def get_safe_workers(step_name: str = "generic") -> int:
    """Return a safe number of threads to avoid overloading APIs."""
    if ENV.lower() in ("dev", "local"):
        return 4
    return MAX_THREADS

# ===============================================================
# 🧩 MODE SUMMARY FUNCTION
# ===============================================================
def print_mode_summary():
    """Print current mode settings for pipeline debugging."""
    print("\n========== PIPELINE MODE SUMMARY ==========")
    print(f"ENVIRONMENT       : {ENV}")
    print(f"USE_GOLDEN_LIST   : {USE_GOLDEN_LIST}")
    print(f"RUN_LOCAL (offline): {RUN_LOCAL}")
    print(f"ALLOW_API_FETCH   : {ALLOW_API_FETCH}")
    print(f"SAVE_RAW_JSON     : {SAVE_RAW_JSON}")
    print(f"DISCOG_MAX_TITLES : {DISCOG_MAX_TITLES}")
    print(f"TITLE_LIST_PATH   : {TITLE_LIST_PATH if TITLE_LIST_PATH.exists() else '(not found)'}")
    print(f"API_TIMEOUT       : {API_TIMEOUT}s, RETRIES={API_MAX_RETRIES}")
    print("===========================================\n")

# ===============================================================
# ✅ POST-LOAD TEST
# ===============================================================
if __name__ == "__main__":
    print_mode_summary()
    titles = get_active_title_list()
    print(f"Loaded {len(titles)} titles for processing.")


# ===============================================================
# ✅ TOKEN MISMATCH TEST
# ===============================================================

# --- Optional: warn if token mismatch between system and .env file ---
def _warn_if_env_mismatch(var_name: str):
    """Compare .env value with active env var; log a warning if they differ."""
    logger = logging.getLogger("config")
    try:
        # 1️⃣ what python-dotenv just loaded into process env
        active_val = os.getenv(var_name)
        # 2️⃣ what’s explicitly in .env (if present)
        file_val = None
        if dotenv_path and Path(dotenv_path).exists():
            for line in Path(dotenv_path).read_text(encoding="utf-8").splitlines():
                if line.startswith(f"{var_name}="):
                    file_val = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break
        if active_val and file_val and active_val[:8] != file_val[:8]:
            logger.warning(
                f"⚠️ {var_name} mismatch: loaded '{active_val[:8]}…' "
                f"but .env has '{file_val[:8]}…' — using active value."
            )
    except Exception as e:
        logger.warning(f"⚠️ Unable to verify {var_name} consistency: {e}")

# --- Call once for critical tokens ---
_warn_if_env_mismatch("DISCOGS_TOKEN")
_warn_if_env_mismatch("TMDB_API_KEY")
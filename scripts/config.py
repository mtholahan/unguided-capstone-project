"""
config.py
---------------------------------------------------------------
Central configuration for the Discogs→TMDB Data Pipeline
(Springboard Unguided Capstone Project)

Version:
    v3.3 — Nov 2025 (UC Auto-Detection)
Purpose:
    - Restore all previously used constants
    - Unify "mode control" logic (golden vs active, local vs API)
    - Auto-detect Unity Catalog vs legacy ADLS key access
    - Maintain backward compatibility for Steps 01–05
---------------------------------------------------------------
"""

# ===============================================================
# ⚙️  SPARK INITIALIZATION (Mount-less Safe)
# ===============================================================
from pyspark.sql import SparkSession

try:
    spark  # noqa: F821
except NameError:
    spark = (
        SparkSession.builder
        .appName("ConfigBootstrap")
        .getOrCreate()
    )
    print("⚙️ Created new SparkSession for config.py")

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

# ===============================================================
# 📦  IMPORTS & ENV LOADING
# ===============================================================
import os, logging, multiprocessing
from pathlib import Path

try:
    from dotenv import load_dotenv, find_dotenv
    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        load_dotenv(dotenv_path, override=True)
        print(f"Loaded local .env from: {dotenv_path}")
    else:
        print("No .env file found — continuing without it.")
except ModuleNotFoundError:
    dotenv_path = None
    print("dotenv not available — skipping (Databricks mode)")

# ===============================================================
# ☁️  STORAGE CONFIGURATION
# ===============================================================
def is_unity_catalog_enabled(spark_session):
    try:
        flag = spark_session.conf.get("spark.databricks.unityCatalog.enabled", "")
        return flag.lower() in ("true", "1", "yes", "y")
    except Exception:
        return False

UC_MODE = is_unity_catalog_enabled(spark)
print("🔗 Unity Catalog detected — passthrough mode."
      if UC_MODE else "🧩 Legacy ADLS mode — using secret-key config.")

# ─ Storage Account
try:
    STORAGE_ACCOUNT = dbutils.secrets.get("markscope", "azure-storage-account-name").strip()
except Exception:
    STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "<your-storage-account>").strip()

# ─ Containers (explicit separation)
CONTAINER_RAW         = "raw"
CONTAINER_INTERMEDIATE = "intermediate"
CONTAINER_METRICS     = "metrics"

# ─ ABFSS paths
RAW_DIR_REMOTE         = f"abfss://{CONTAINER_RAW}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
INTERMEDIATE_DIR_REMOTE = f"abfss://{CONTAINER_INTERMEDIATE}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
METRICS_DIR_REMOTE     = f"abfss://{CONTAINER_METRICS}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

if not UC_MODE:
    try:
        key = dbutils.secrets.get("markscope", "azure-storage-account-key").strip()
    except Exception:
        key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
    if key:
        spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", key)
        print(f"🔑 Configured key-based access for {STORAGE_ACCOUNT}")
    else:
        print("⚠️ No storage key found; ADLS access may fail.")
else:
    print(f"✅ Using UC passthrough for {STORAGE_ACCOUNT}")

# ===============================================================
# 🗂️  LOCAL FALLBACKS
# ===============================================================
# NOTE:
# - We only use plain strings for ABFSS and local paths (no pathlib.Path)
# - Prevents "TypeError: unsupported operand type(s) for /: 'str' and 'str'"
# - Fully Databricks + Azure Blob compatible

# --- Root and local folders (for fallback/testing)
ROOT_DIR = str(Path(__file__).resolve().parents[1])
DATA_DIR = f"{ROOT_DIR}/data"
LOCAL_PATHS = {
    "raw": f"{DATA_DIR}/raw",
    "intermediate": f"{DATA_DIR}/intermediate",
    "metrics": f"{DATA_DIR}/metrics",
    "processed": f"{DATA_DIR}/processed",
    "logs": f"{ROOT_DIR}/logs",
}

for path_str in LOCAL_PATHS.values():
    os.makedirs(path_str, exist_ok=True)

# --- ADLS Locations (always strings)
RAW_DIR = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
INTERMEDIATE_DIR = f"abfss://intermediate@{STORAGE_ACCOUNT}.dfs.core.windows.net"
METRICS_DIR = f"abfss://metrics@{STORAGE_ACCOUNT}.dfs.core.windows.net"

LOG_DIR = f"{DATA_DIR}/logs"
os.makedirs(LOG_DIR, exist_ok=True)

CPU_CORES = multiprocessing.cpu_count()
ENV = os.getenv("ENV", "dev")

print(f"📁 DATA ROOT : {DATA_DIR}")
print(f"🌐 RAW_DIR          → {RAW_DIR}")
print(f"🌐 INTERMEDIATE_DIR → {INTERMEDIATE_DIR}")
print(f"🌐 METRICS_DIR      → {METRICS_DIR}")

# ===============================================================
# 🎛️  PIPELINE MODE CONTROLS
# ===============================================================
USE_GOLDEN_LIST = True
#TITLE_LIST_PATH = DATA_DIR / "movie_titles_200.txt"
TITLE_LIST_PATH = f"{DATA_DIR}/movie_titles_200.txt"


RUN_LOCAL = False
FORCE_CACHE_ONLY = RUN_LOCAL
SAVE_RAW_JSON = True
ALLOW_API_FETCH = not RUN_LOCAL

DISCOG_MAX_TITLES = 50
TMDB_MAX_RESULTS = 5
MAX_THREADS = int(os.getenv("MAX_THREADS", min(8, CPU_CORES * 2)))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ===============================================================
# 🕐  API TIMEOUTS / RETRIES
# ===============================================================
API_TIMEOUT = 20
API_MAX_RETRIES = 3
RETRY_BACKOFF = 2.0
TMDB_REQUEST_DELAY_SEC = 0.8

# ===============================================================
# 🎞️  DISCOGS SETTINGS
# ===============================================================
DISCOGS_API_URL = "https://api.discogs.com/database/search"
DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN", "")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "UnguidedCapstoneBot/1.0")

#DISCOGS_RAW_DIR = RAW_DIR / "discogs_raw"
DISCOGS_RAW_DIR = f"{RAW_DIR}/discogs_raw"
#DISCOGS_RAW_DIR.mkdir(parents=True, exist_ok=True)
os.makedirs(DISCOGS_RAW_DIR, exist_ok=True)

DISCOGS_MAX_RETRIES = 3
DISCOGS_PER_PAGE = 5
DISCOGS_SLEEP_SEC = 2.0
RATE_LIMIT_SLEEP_SEC = 60

# ===============================================================
# 🎥  TMDB SETTINGS
# ===============================================================
TMDB_API_URL = "https://api.themoviedb.org/3/search/movie"
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_SLEEP_SEC = TMDB_REQUEST_DELAY_SEC
#TMDB_RAW_DIR = RAW_DIR / "tmdb_raw"
TMDB_RAW_DIR = f"{RAW_DIR}/tmdb_raw"
#TMDB_RAW_DIR.mkdir(parents=True, exist_ok=True)
os.makedirs(TMDB_RAW_DIR, exist_ok=True)

# ===============================================================
# 🧩  STEP-SPECIFIC PARAMETERS
# ===============================================================
TMDB_SEARCH_URL = TMDB_API_URL
TMDB_RATE_LIMIT = 40
DEFAULT_MAX_WORKERS = MAX_THREADS
FUZZ_THRESHOLD = 85
YEAR_VARIANCE = 2
TOP_N = 5

# ===============================================================
# 🎬  GOLDEN TITLE LISTS
# ===============================================================
GOLDEN_TITLES = [
    "Inception","Interstellar","The Dark Knight","Blade Runner","The Matrix",
    "Pulp Fiction","Forrest Gump","The Godfather","The Shawshank Redemption","Fight Club",
    "Back to the Future","Gladiator","Titanic","Avatar","Jurassic Park",
    "Star Wars","The Lord of the Rings","Harry Potter","La La Land","The Lion King","Frozen","Jaws"
]
GOLDEN_TITLES_TEST = GOLDEN_TITLES[:10]

# ===============================================================
# 🎬  TITLE SOURCE RESOLVER
# ===============================================================
def get_active_title_list(path=None):
    import pandas as pd
    file_path = Path(path or TITLE_LIST_PATH)

    if USE_GOLDEN_LIST:
        print("[Config] Using curated GOLDEN_TITLES list.")
        return GOLDEN_TITLES
    if file_path.exists():
        try:
            if file_path.suffix.lower() == ".csv":
                df = pd.read_csv(file_path)
                titles = df.iloc[:, 0].dropna().astype(str).tolist()
            else:
                titles = [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]
            print(f"[Config] Loaded {len(titles)} active titles from {file_path.name}.")
            return titles
        except Exception as e:
            print(f"[Config] ⚠️ Failed to read {file_path}: {e}")
    if ENV.lower() in ("dev","local"):
        print("[Config] ⚠️ Dev fallback → GOLDEN_TITLES_TEST.")
        return GOLDEN_TITLES_TEST
    raise FileNotFoundError(f"❌ Title list file not found: {file_path}")

# ===============================================================
# 🧮  WORKER MANAGEMENT
# ===============================================================
def get_safe_workers(step_name="generic") -> int:
    return 4 if ENV.lower() in ("dev","local") else MAX_THREADS

# ===============================================================
# 🧩  MODE SUMMARY
# ===============================================================
def print_mode_summary():
    print("\n========== PIPELINE MODE SUMMARY ==========")
    print(f"ENVIRONMENT        : {ENV}")
    print(f"USE_GOLDEN_LIST    : {USE_GOLDEN_LIST}")
    print(f"RUN_LOCAL (offline): {RUN_LOCAL}")
    print(f"ALLOW_API_FETCH    : {ALLOW_API_FETCH}")
    print(f"SAVE_RAW_JSON      : {SAVE_RAW_JSON}")
    print(f"DISCOG_MAX_TITLES  : {DISCOG_MAX_TITLES}")
    # print(f"TITLE_LIST_PATH    : {TITLE_LIST_PATH if TITLE_LIST_PATH.exists() else '(not found)'}")
    if hasattr(TITLE_LIST_PATH, "exists"):
        exists_flag = TITLE_LIST_PATH.exists()
    else:
        exists_flag = False
    print(f"TITLE_LIST_PATH : {TITLE_LIST_PATH} {'(exists)' if exists_flag else '(remote or not found)'}")
    print(f"API_TIMEOUT        : {API_TIMEOUT}s  RETRIES={API_MAX_RETRIES}")
    print("===========================================\n")

# ===============================================================
# ✅  POST-LOAD TEST
# ===============================================================
if __name__ == "__main__":
    print_mode_summary()
    titles = get_active_title_list()
    print(f"Loaded {len(titles)} titles for processing.")

# ===============================================================
# ✅  TOKEN-MISMATCH WARNER
# ===============================================================
def _warn_if_env_mismatch(var_name: str):
    logger = logging.getLogger("config")
    try:
        active_val = os.getenv(var_name)
        file_val = None
        if dotenv_path and Path(dotenv_path).exists():
            for line in Path(dotenv_path).read_text(encoding="utf-8").splitlines():
                if line.startswith(f"{var_name}="):
                    file_val = line.split("=",1)[1].strip().strip('"').strip("'")
                    break
        if active_val and file_val and active_val[:8] != file_val[:8]:
            logger.warning(
                f"⚠️ {var_name} mismatch: env='{active_val[:8]}…' "
                f"vs .env='{file_val[:8]}…' — using active value."
            )
    except Exception as e:
        logger.warning(f"⚠️ Unable to verify {var_name}: {e}")

_warn_if_env_mismatch("DISCOGS_TOKEN")
_warn_if_env_mismatch("TMDB_API_KEY")

# ===============================================================
# ✅  Helpers
# ===============================================================

def join_uri(base, subpath):
    """Safely join ADLS URIs or local Paths."""
    if isinstance(base, str):
        return f"{base.rstrip('/')}/{subpath.lstrip('/')}"
    from pathlib import Path
    return Path(base) / subpath


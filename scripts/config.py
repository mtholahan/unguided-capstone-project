"""
config.py
----------------------------------------------------------------
Central configuration for the Discogs → TMDB Data Pipeline
(Springboard Unguided Capstone Project)

Version:
    v3.4 — Nov 2025 (Step 9 Full-Scale Refactor)
Purpose:
    - Cluster all "Full-Scale Switches" at the top
    - Preserve all legacy constants and backward compatibility
    - Maintain unified environment + storage config
    - Auto-detect Unity Catalog vs legacy ADLS access
----------------------------------------------------------------
"""

import os 
import logging
import multiprocessing
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark
import os
from datetime import datetime

# ================================================================
# 🧭 Environment-Aware Configuration Switcher
# ---------------------------------------------------------------
# Allows toggling between 'test' (light) and 'prod' (full-scale)
# via ENV= environment variable.
# ================================================================

ENV = os.getenv("ENV", "test").lower()  # defaults to 'test' if unset
IS_PROD = ENV in ["prod", "production"]
IS_TEST = not IS_PROD

RUN_LOCAL = os.getenv("RUN_LOCAL", "false").lower() == "true"

print(f"🔧 Config initialized for environment: {ENV.upper()}")

# ================================================================
# 🎬 TMDB Extraction Parameters
# ================================================================
if IS_PROD:
    TMDB_PAGE_LIMIT = 500                # int — Number of TMDB pages to request
    TMDB_MAX_RESULTS = 20                # int — Number of movie results returned per TMDB page.
    TMDB_REQUEST_DELAY_SEC = 0.5         # float — Delay (in seconds) between sequential TMDB API calls.
else:
    TMDB_PAGE_LIMIT = 2
    TMDB_MAX_RESULTS = 20
    TMDB_REQUEST_DELAY_SEC = 0.3

# ================================================================
# 💿 Discogs Extraction Parameters
# ================================================================
if IS_PROD:
    DISCOGS_PAGE_CAP = 75                 # int — Maximum number of pages to fetch per Discogs query term.
    DISCOGS_PER_PAGE = 100                # int — Number of results requested per page from the Discogs API.
    DISCOGS_SLEEP_SEC = 0.6               # float — Delay (in seconds) between Discogs page requests.
    DISCOGS_MAX_TITLES = 15_000            # int — Hard cap on total Discogs records to retain post-extraction.
    DISCOGS_USER_AGENT = "DataPipelineProd/1.0"
else:
    DISCOGS_PAGE_CAP = 5
    DISCOGS_PER_PAGE = 100
    DISCOGS_SLEEP_SEC = 1.0
    DISCOGS_MAX_TITLES = 2_000
    DISCOGS_USER_AGENT = "DataPipelineTest/1.0"

# A list of search queries (genres, artists, or keywords)
DISCOGS_QUERY = ["soundtrack", "film soundtrack", "motion picture", "score"]


# ================================================================
# Ye Ol' Golden List
# ================================================================
USE_GOLDEN_LIST = True


# ================================================================
# 🌐 Shared Network Reliability
# ================================================================
if IS_PROD:
    API_TIMEOUT = 30
    API_MAX_RETRIES = 8
    RETRY_BACKOFF = 2.0
    MAX_PAGINATION_WARN = 500
else:
    API_TIMEOUT = 15
    API_MAX_RETRIES = 3
    RETRY_BACKOFF = 1.0
    MAX_PAGINATION_WARN = 100


# Thread & log config
CPU_CORES = multiprocessing.cpu_count()
#MAX_THREADS = int(os.getenv("MAX_THREADS", CPU_CORES * 2))
MAX_THREADS = int(os.getenv("MAX_THREADS", CPU_CORES))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Derived flags
FORCE_CACHE_ONLY = RUN_LOCAL
SAVE_RAW_JSON = True
ALLOW_API_FETCH = not RUN_LOCAL


# ===============================================================
# ⚙️  SPARK INITIALIZATION (Driver-safe)
# ===============================================================

spark = None
try:
    # Only create a SparkSession if we're on the driver
    # (executors have a TaskContext; drivers do not)
    if pyspark.TaskContext.get() is None:
        spark = (
            SparkSession.builder
            .appName("ConfigBootstrap")
            .getOrCreate()
        )
        print("⚙️ Created new SparkSession for config.py (driver mode)")
    else:
        print("ℹ️ Skipping SparkSession init inside executor (worker mode)")

except Exception as e:
    print(f"⚠️ SparkSession initialization skipped or failed: {e}")

# ---------------------------------------------------------------
# Databricks Utilities (only on driver and within Databricks)
# ---------------------------------------------------------------
if os.getenv("DATABRICKS_RUNTIME_VERSION") and spark is not None:
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except Exception as e:
        dbutils = None
        print(f"⚠️ DBUtils import failed: {e}")
else:
    dbutils = None
    print("⚠️ Running outside Databricks or on executor – skipping DBUtils import.")


# ===============================================================
# ☁️  STORAGE CONFIGURATION (Unity Catalog / ADLS)
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

# ===============================================================
# 🧭 STORAGE ACCOUNT DISCOVERY
# ===============================================================

try:
    STORAGE_ACCOUNT = dbutils.secrets.get("markscope", "azure-storage-account-name").strip()
except Exception:
    STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "<your-storage-account>").strip()

# Containers (Medallion + Metrics)
CONTAINER_BRONZE = "raw"            # Bronze = raw ingestion
CONTAINER_SILVER = "intermediate"   # Silver = cleansed / normalized
CONTAINER_GOLD   = "gold"           # Gold = curated / enriched
CONTAINER_METRICS = "metrics"

# ===============================================================
# 🔐 AUTHENTICATION MODE
# ===============================================================

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
# 📂 MEDALLION LAYER PATHS
# ===============================================================

BRONZE_DIR  = f"abfss://{CONTAINER_BRONZE}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_DIR  = f"abfss://{CONTAINER_SILVER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_DIR    = f"abfss://{CONTAINER_GOLD}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
METRICS_DIR = f"abfss://{CONTAINER_METRICS}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# ===============================================================
# 🧰 PATH FACTORY HELPERS
# ===============================================================

RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%S")    # Unique run ID
ENV = os.getenv("ENV", "test").lower()

def layer_path(layer: str, dataset: str) -> str:
    """
    Returns the base path for a dataset within a given Medallion layer.
    Does NOT embed env or run_id (these are handled via partitioning).
    """
    base = {
        "bronze": BRONZE_DIR,
        "silver": SILVER_DIR,
        "gold": GOLD_DIR,
        "metrics": METRICS_DIR,
        "intermediate": "abfss://intermediate@ungcapstor01.dfs.core.windows.net", # Temp
    }.get(layer)

    if not base:
        raise ValueError(f"Invalid layer name: {layer}")

    return f"{base}/{dataset}"

def env_layer_path(layer: str, dataset: str) -> str:
    """Shortcut for current run/env combination."""
    return layer_path(layer, dataset, RUN_ID)

def write_df(
    df,
    layer: str,
    dataset: str,
    mode: str = "overwrite",
    partition_cols: list | None = None
):
    """
    Unified write method for all Medallion layers.
    Supports optional partitioning (env/run_id or custom).
    Automatically resolves base path via layer_path().
    """

    path = layer_path(layer, dataset)

    # Default partitioning for Bronze/Silver if none provided
    if partition_cols is None and layer in ["bronze", "silver"]:
        partition_cols = ["env", "run_id"]

    writer = df.write.mode(mode)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    print(f"DEBUG → Writing {dataset} ({layer}) | partition_cols={partition_cols}")
    print(f"DEBUG → Schema before write: {df.columns}")

    writer.parquet(path)

    print(f"✅ Wrote DataFrame to {path} "
          f"({'partitioned by ' + ', '.join(partition_cols) if partition_cols else 'flat'})")

    return path


def get_paths_dict():
    """Expose current medallion directories and context."""
    return {
        "bronze": BRONZE_DIR,
        "silver": SILVER_DIR,
        "gold": GOLD_DIR,
        "metrics": METRICS_DIR,
        "storage_account": STORAGE_ACCOUNT,
        "env": ENV,
        "run_id": RUN_ID,
        "unity_catalog": UC_MODE,
    }

# ===============================================================
# 🖥️ LOCAL DEV SUPPORT (optional)
# ===============================================================

ROOT_DIR = str(Path(__file__).resolve().parents[1])
DATA_DIR = f"{ROOT_DIR}/data"

LOCAL_PATHS = {
    "bronze": f"{DATA_DIR}/raw",
    "silver": f"{DATA_DIR}/intermediate",
    "gold": f"{DATA_DIR}/gold",
    "metrics": f"{DATA_DIR}/metrics",
}
for p in LOCAL_PATHS.values():
    os.makedirs(p, exist_ok=True)

if os.getenv("LOCAL_MODE", "false").lower() == "true":
    BRONZE_DIR, SILVER_DIR, GOLD_DIR, METRICS_DIR = (
        LOCAL_PATHS["bronze"],
        LOCAL_PATHS["silver"],
        LOCAL_PATHS["gold"],
        LOCAL_PATHS["metrics"],
    )
    print("🧩 Local dev mode enabled — writing to local /data folder.")

# ===============================================================
# 📊 SUMMARY
# ===============================================================

print("🔗 STORAGE SUMMARY")
print(f"📦 Bronze  → {BRONZE_DIR}")
print(f"📦 Silver  → {SILVER_DIR}")
print(f"📦 Gold    → {GOLD_DIR}")
print(f"📊 Metrics → {METRICS_DIR}")
print(f"🌍 Mode: {'Unity Catalog' if UC_MODE else 'ADLS'} | ENV={ENV} | RUN_ID={RUN_ID}")


# ===============================================================
# 🎞️  API CONFIG (Shared)
# ===============================================================
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
TMDB_SLEEP_SEC = TMDB_REQUEST_DELAY_SEC

DISCOGS_TOKEN = os.getenv("DISCOGS_TOKEN", "")
DISCOGS_USER_AGENT = os.getenv("DISCOGS_USER_AGENT", "UnguidedCapstoneBot/1.0")


# ===============================================================
# 🧩  STEP-SPECIFIC PARAMETERS
# ===============================================================
DEFAULT_MAX_WORKERS = MAX_THREADS
FUZZ_THRESHOLD = 85
YEAR_VARIANCE = 3
TOP_N = 5   # probably unused

# ===============================================================
# 🎬  GOLDEN TITLES / ACTIVE LISTS
# ===============================================================
TITLE_LIST_PATH = f"{DATA_DIR}/movie_titles_200.txt"

GOLDEN_TITLES = [
    "Inception","Interstellar","The Dark Knight","Blade Runner","The Matrix",
    "Pulp Fiction","Forrest Gump","The Godfather","The Shawshank Redemption","Fight Club",
    "Back to the Future","Gladiator","Titanic","Avatar","Jurassic Park",
    "Star Wars","The Lord of the Rings","Harry Potter","La La Land","The Lion King","Frozen","Jaws"
]
GOLDEN_TITLES_TEST = GOLDEN_TITLES[:10]

def get_active_title_list(path=None):
    """Resolve which title list to use."""
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
                titles = [t.strip() for t in file_path.read_text(encoding="utf-8").splitlines() if t.strip()]
            print(f"[Config] Loaded {len(titles)} active titles from {file_path.name}.")
            return titles
        except Exception as e:
            print(f"[Config] ⚠️ Failed to read {file_path}: {e}")

    if ENV.lower() in ("dev", "local"):
        print("[Config] ⚠️ Dev fallback → GOLDEN_TITLES_TEST.")
        return GOLDEN_TITLES_TEST

    raise FileNotFoundError(f"❌ Title list file not found: {file_path}")

# ===============================================================
# 🧮  WORKER MGMT
# ===============================================================
def get_safe_workers(step_name="generic") -> int:
    """Return safe worker count for environment."""
    return 4 if ENV.lower() in ("dev", "local") else MAX_THREADS

# ===============================================================
# 🧩  MODE SUMMARY
# ===============================================================
def print_mode_summary():
    print("\n⚙️ Active Configuration Summary:")
    print(f"  ENV Mode          : {ENV.upper()}")
    print(f"  TMDB_PAGE_LIMIT   : {TMDB_PAGE_LIMIT}")
    print(f"  TMDB_MAX_RESULTS  : {TMDB_MAX_RESULTS}")
    print(f"  TMDB_DELAY_SEC    : {TMDB_REQUEST_DELAY_SEC}")
    print(f"  DISCOGS_PAGE_CAP  : {DISCOGS_PAGE_CAP}")
    print(f"  DISCOGS_SLEEP_SEC : {DISCOGS_SLEEP_SEC}")
    print(f"  API_TIMEOUT       : {API_TIMEOUT}s")
    print(f"  MAX_RETRIES       : {API_MAX_RETRIES}")
    print(f"  RETRY_BACKOFF     : {RETRY_BACKOFF}\n")
    print("===========================================\n")

# ===============================================================
# ✅  TOKEN-MISMATCH WARNER
# ===============================================================
def _warn_if_env_mismatch(var_name: str):
    logger = logging.getLogger("config")
    dotenv_path = Path(".env")
    try:
        active_val = os.getenv(var_name)
        file_val = None
        if dotenv_path.exists():
            for line in dotenv_path.read_text(encoding="utf-8").splitlines():
                if line.startswith(f"{var_name}="):
                    file_val = line.split("=", 1)[1].strip().strip('"').strip("'")
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
# ✅  HELPERS
# ===============================================================
def join_uri(base, subpath):
    """Safely join ADLS URIs or local Paths."""
    if isinstance(base, str):
        return f"{base.rstrip('/')}/{subpath.lstrip('/')}"
    from pathlib import Path
    return Path(base) / subpath

# ===============================================================
# ✅  SELF-TEST
# ===============================================================
if __name__ == "__main__":
    print_mode_summary()
    titles = get_active_title_list()
    print(f"Loaded {len(titles)} titles for processing.")

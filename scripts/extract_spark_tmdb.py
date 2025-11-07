# ================================================================
#  extract_spark_tmdb.py — v4.2 (canonical-id + full logging)
#  ---------------------------------------------------------------
#  Purpose : Extract popular TMDB movies and write Parquet to ADLS
#  Runtime : Databricks 16.4 LTS (Spark-based)
#  Author  : M. Holahan
# ================================================================

import scripts.bootstrap  # Ensures package discovery (local + Databricks)
from scripts import config
from scripts.base_step import BaseStep
from pyspark.sql import functions as F, types as T
import os, time, json, requests


# ================================================================
#  🔧 Runtime Configuration and Shared Constants
# ================================================================

spark = config.spark   # Active Spark session or context

# ----------------------------------------------------------------
# 🗂️ Output and API Configuration
# ----------------------------------------------------------------
OUTPUT_PATH = config.layer_path("bronze", "tmdb")
TMDB_API_URL = "https://api.themoviedb.org/3/movie/popular"

# ----------------------------------------------------------------
# ⚙️ Pagination & Throttling Controls
# ----------------------------------------------------------------
PAGE_LIMIT       = config.TMDB_PAGE_LIMIT          # total pages to pull (TMDB global cap)
RESULTS_PER_PAGE = config.TMDB_MAX_RESULTS         # results returned per TMDB page
REQUEST_DELAY    = config.TMDB_REQUEST_DELAY_SEC   # delay between TMDB requests (seconds)

# ----------------------------------------------------------------
# 🌐 Network Reliability Controls
# ----------------------------------------------------------------
API_TIMEOUT        = config.API_TIMEOUT             # Timeout (seconds) for API request completion
MAX_RETRIES        = config.API_MAX_RETRIES         # Maximum retry attempts per failed request
RETRY_BACKOFF      = config.RETRY_BACKOFF           # Exponential backoff multiplier between retries
MAX_PAGINATION_WARN = config.MAX_PAGINATION_WARN    # Global safety bound to prevent runaway pagination


# ================================================================
#  Extract TMDB Step
# ================================================================
class Step01ExtractSparkTMDB(BaseStep):
    """
    Step 01 – Extract TMDB movie data and store to ADLS Bronze layer (Parquet).

    This step:
    • Pulls paginated movie metadata from TMDB's public API.
    • Adds standard lineage columns (env, run_id, ingest_ts).
    • Generates a deterministic canonical_id for cross-layer joins.
    • Writes partitioned Bronze data to the Medallion architecture.
    """

    def __init__(self):
        """Initialize step context and logger."""
        super().__init__("step_01_extract_spark_tmdb")
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("✅ Initialized Step 01 (config-driven, mount-less)")

    # ------------------------------------------------------------
    def _get_tmdb_api_key(self) -> str:
        """Retrieve TMDB API key from Databricks secret scope or environment."""
        try:
            api_key = config.dbutils.secrets.get("markscope", "tmdb-api-key").strip()
        except Exception:
            api_key = os.getenv("TMDB_API_KEY", "").strip()
        if not api_key:
            raise ValueError("❌ TMDB API key not found in secrets or environment variables.")
        return api_key

    # ------------------------------------------------------------
    def _fetch_page(self, api_key: str, page: int) -> dict | None:
        """
        Fetch one TMDB page using simple retry/backoff logic.
        Returns a JSON dictionary or None if the page fails permanently.
        """
        url = f"{TMDB_API_URL}?api_key={api_key}&language=en-US&page={page}"
        attempts = 0
        while attempts <= MAX_RETRIES:
            try:
                resp = requests.get(url, timeout=API_TIMEOUT)
                if resp.status_code == 200:
                    return resp.json()
                self.logger.warning(f"⚠️ TMDB page {page} failed ({resp.status_code}). Retrying...")
            except Exception as e:
                self.logger.warning(f"⚠️ Exception fetching page {page}: {e}")
            attempts += 1
            time.sleep(RETRY_BACKOFF)
        self.logger.error(f"❌ TMDB page {page} failed after {MAX_RETRIES} retries.")
        return None

    # ------------------------------------------------------------
    def run(self, _: dict | None = None):
        """
        Run the TMDB extraction pipeline.
        1. Retrieve paginated movie data from TMDB.
        2. Parse and explode JSON into Spark DataFrame.
        3. Append canonical_id + lineage metadata.
        4. Write Bronze Parquet output (partitioned by env/run_id).
        """
        t0 = time.time()
        self.logger.info("🚀 Starting TMDB extraction job")

        # (1) Acquire API key
        api_key = self._get_tmdb_api_key()
        self.logger.info("🔑 TMDB API key loaded successfully.")

        # (2) Download TMDB pages
        all_pages = []
		
        for page in range(1, PAGE_LIMIT + 1):
            payload = self._fetch_page(api_key, page)
            if payload:
                results = payload.get("results", [])
                all_pages.append(payload)
                self.logger.info(f"📥 Page {page:>3} retrieved → {len(results)} results.")
            else:
                self.logger.warning(f"⚠️ Skipping page {page}: no payload returned.")
            time.sleep(REQUEST_DELAY)

        if not all_pages:
            raise RuntimeError("❌ No TMDB data retrieved — check API key or network connectivity.")

        # (3) Convert JSON pages → Spark DataFrame
        json_rows = [(json.dumps(p),) for p in all_pages]
        json_schema = T.StructType([T.StructField("json_data", T.StringType())])

        df_raw = (
            self.spark.createDataFrame(json_rows, json_schema)
            .withColumn("ingest_ts", F.current_timestamp())
        )
        self.logger.info(f"📦 Loaded {len(all_pages)} JSON page objects into Spark.")

        schema_results = T.StructType([
            T.StructField("page", T.IntegerType()),
            T.StructField("results", T.ArrayType(T.StructType([
                T.StructField("id", T.IntegerType()),
                T.StructField("title", T.StringType()),
                T.StructField("release_date", T.StringType()),
                T.StructField("genre_ids", T.ArrayType(T.IntegerType())),
            ]))),
        ])

        df_exploded = (
            df_raw
            .withColumn("json", F.from_json("json_data", schema_results))
            .withColumn("movie", F.explode("json.results"))
        )

        df_tmdb = (
            df_exploded
            .select(
                F.col("movie.id").alias("tmdb_id"),
                F.col("movie.title").alias("tmdb_title"),
                F.col("movie.release_date").alias("tmdb_release_date"),
                F.col("movie.genre_ids").alias("tmdb_genre_ids"),
                F.col("ingest_ts")
            )
            .withColumn("tmdb_year", F.substring(F.col("tmdb_release_date"), 1, 4).cast("int"))
            .dropna(subset=["tmdb_title"])
            .withColumn("env", F.lit(config.ENV))
            .withColumn("run_id", F.lit(config.RUN_ID))
        )
        self.logger.info(f"🧮 Parsed TMDB movies → {df_tmdb.count()} records before canonical ID.")

        # (4) Append canonical_id for lineage
        df_tmdb = df_tmdb.withColumn(
            "canonical_id",
            F.sha2(
                F.concat_ws("|",
                    F.coalesce(F.col("tmdb_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("tmdb_title"), F.lit("")),
                    F.coalesce(F.col("tmdb_year").cast("string"), F.lit(""))
                ), 256
            )
        )
        self.logger.info("🔗 Appended canonical_id (SHA-256 hash of tmdb_id|title|year).")

        # (5) Persist to Bronze
		
        self.logger.info(f"💾 Writing to Bronze path: {OUTPUT_PATH}")
        config.write_df(df_tmdb, "bronze", "tmdb", partition_cols=["env", "run_id"])

        count = df_tmdb.count()
        self.logger.info(
            f"✅ Wrote {count} TMDB records → Bronze layer "
            f"({config.ENV}, run_id={config.RUN_ID})"
        )
        self.logger.info(f"📦 Bronze path: {OUTPUT_PATH}")

        # (6) Metrics
        metrics = {
            "titles_total": count,
            "records_written": count,
            "duration_sec": round(time.time() - t0, 2),
            "output_path": OUTPUT_PATH,
            "page_limit": PAGE_LIMIT,
            "env": config.ENV,
            "run_id": config.RUN_ID,
            "step_name": "extract_spark_tmdb_metrics",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        metrics_path = config.layer_path("metrics", "extract_spark_tmdb")
        self.write_metrics(metrics, name="extract_spark_tmdb_metrics", metrics_dir=metrics_path)
        self.logger.info(f"📊 Logged TMDB extract metrics to {metrics_path}")
        self.logger.info(f"✅ Step 01 completed successfully in {metrics['duration_sec']} s")

        return df_tmdb


# ================================================================
#  Entrypoint for Databricks / Local
# ================================================================
if __name__ == "__main__":
    Step01ExtractSparkTMDB().run(None)
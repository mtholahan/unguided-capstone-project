# ================================================================
#  _extract_spark_discogs.py — v4.2 (canonical-id + full logging)
#  ---------------------------------------------------------------
#  Purpose : Extract Discogs releases and write Parquet to ADLS
#  Runtime : Databricks 16.4 LTS (Spark-based)
#  Author  : M. Holahan
# ================================================================

import scripts.bootstrap  # Ensures package discovery on Databricks/local
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
OUTPUT_PATH = config.layer_path("bronze", "discogs")
DISCOGS_API_URL = "https://api.discogs.com/database/search"
DISCOGS_USER_AGENT = config.DISCOGS_USER_AGENT

# ----------------------------------------------------------------
# 🎚️ Pagination & Throttling Controls
# ----------------------------------------------------------------
DISCOGS_PAGE_CAP      = config.DISCOGS_PAGE_CAP      # Max number of pages to request per query (prevents runaway loops)
DISCOGS_PER_PAGE      = config.DISCOGS_PER_PAGE      # Number of releases returned per API page (Discogs default = 100)
DISCOGS_SLEEP_SEC     = config.DISCOGS_SLEEP_SEC     # Delay between page requests (seconds) for rate-limit compliance
DISCOGS_MAX_TITLES    = config.DISCOGS_MAX_TITLES    # Global safety cap on total titles fetched across all queries
MAX_PAGINATION_WARN   = config.MAX_PAGINATION_WARN   # Failsafe to warn if pagination exceeds safe bounds


# ----------------------------------------------------------------
# 🌐 Network Reliability Controls
# ----------------------------------------------------------------
API_TIMEOUT   = config.API_TIMEOUT
API_MAX_RETRIES   = config.API_MAX_RETRIES
RETRY_BACKOFF = config.RETRY_BACKOFF


# ================================================================
#  Step Definition
# ================================================================
class Step02ExtractSparkDiscogs(BaseStep):
    """
    Step 02 – Extract Discogs soundtrack data and store to ADLS Bronze layer.

    This step:
    • Queries the Discogs API for soundtrack-related releases.
    • Handles pagination, retries, and backoff.
    • Adds lineage fields (env, run_id, ingest_ts).
    • Appends a canonical_id hash for deterministic identity across runs.
    • Writes partitioned Parquet output to Bronze.
    """

    def __init__(self):
        """Initialize the Discogs extraction step."""
        super().__init__("step_02_extract_spark_discogs")
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")
        self.metrics_dir = config.METRICS_DIR
        self.logger.info("✅ Initialized Step 02 (config-driven, mount-less)")

    # ------------------------------------------------------------
    def _get_credentials(self):
        """Retrieve Discogs API key/secret from Databricks secrets or environment."""
        try:
            key = config.dbutils.secrets.get("markscope", "discogs-consumer-key").strip()
            secret = config.dbutils.secrets.get("markscope", "discogs-consumer-secret").strip()
        except Exception:
            key = os.getenv("DISCOGS_CONSUMER_KEY", "").strip()
            secret = os.getenv("DISCOGS_CONSUMER_SECRET", "").strip()
        if not (key and secret):
            raise ValueError("❌ Discogs API credentials not found in secrets or environment.")
        return key, secret

    # ------------------------------------------------------------
    def _fetch_page(self, query, key, secret, page):
        """Fetch one Discogs search page with retry/backoff."""
        params = {
            "q": query,
            "type": "release",
            "per_page": DISCOGS_PER_PAGE,
            "page": page,
            "key": key,
            "secret": secret,
        }

        for attempt in range(1, API_MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    DISCOGS_API_URL,
                    params=params,
                    timeout=API_TIMEOUT,
                    headers={"User-Agent": DISCOGS_USER_AGENT},
                )

                if resp.status_code == 200:
                    data = resp.json()
                    total_pages = min(data.get("pagination", {}).get("pages", 1), MAX_PAGINATION_WARN)
                    if page > total_pages:
                        self.logger.info(f"🧭 Page {page} exceeds available range ({total_pages}).")
                        return None
                    return data

                if resp.status_code == 429:
                    sleep_time = float(resp.headers.get("Retry-After", 60 * attempt))
                    self.logger.warning(f"🚦 Rate limited. Sleeping {sleep_time:.1f}s.")
                    time.sleep(sleep_time)
                    continue

                if 500 <= resp.status_code < 600:
                    self.logger.warning(f"Server {resp.status_code}, retrying...")
                    time.sleep(RETRY_BACKOFF * attempt)
                    continue

                self.logger.error(f"❌ HTTP {resp.status_code}: {resp.text[:200]}")
                break

            except requests.RequestException as e:
                self.logger.warning(f"⚠️ Network error ({type(e).__name__}): {e}. Retrying...")
                time.sleep(RETRY_BACKOFF * attempt)

        self.logger.error(f"❌ Page {page} failed after {API_MAX_RETRIES} retries.")
        return None

    # ------------------------------------------------------------
    def run(self, _: dict | None = None):
        """
        Run the Discogs extraction pipeline.
        1. Retrieve releases for soundtrack-related queries.
        2. Parse JSON results into Spark DataFrame.
        3. Filter for OST / movie score releases.
        4. Append canonical_id + lineage fields.
        5. Write Bronze Parquet dataset partitioned by env/run_id.
        """
        t0 = time.time()
        self.logger.info("🚀 Starting Discogs extraction job")

        key, secret = self._get_credentials()
        self.logger.info("🔑 Discogs API credentials loaded successfully.")

        # (1) Download pages for each query
        all_pages = []
        queries = config.DISCOGS_QUERY if isinstance(config.DISCOGS_QUERY, (list, tuple)) else [config.DISCOGS_QUERY]
        self.logger.info(f"🔍 Using Discogs queries: {queries}")

        page_cap = min(DISCOGS_PAGE_CAP, MAX_PAGINATION_WARN)
		
        for term in queries:
            self.logger.info(f"🎧 Extracting releases for '{term}' (max {page_cap} pages)")
            for page in range(1, page_cap + 1):
                payload = self._fetch_page(term, key, secret, page)
                if not payload:
                    break
                results = payload.get("results", [])
                if not results:
                    break
                all_pages.append({
                    "query": term,
                    "page": payload.get("pagination", {}).get("page", page),
                    "results": results,
                })
                self.logger.info(f"📥 '{term}' page {page}: {len(results)} results")
                time.sleep(DISCOGS_SLEEP_SEC)
            self.logger.info(f"✅ Completed extraction for '{term}' after {page} pages")

        if not all_pages:
            raise RuntimeError("❌ No Discogs data retrieved — check keys or rate limit.")

        # (2) Convert JSON → Spark DataFrame
        json_rows = [(json.dumps(p),) for p in all_pages]
        json_schema = T.StructType([T.StructField("json_data", T.StringType())])
        df_raw = self.spark.createDataFrame(json_rows, json_schema).withColumn("ingest_ts", F.current_timestamp())
        self.logger.info(f"📦 Loaded {len(all_pages)} JSON page objects into Spark.")

        # (3) Parse and explode JSON
        schema_results = T.StructType([
            T.StructField("page", T.IntegerType()),
            T.StructField("results", T.ArrayType(T.StructType([
                T.StructField("id", T.IntegerType()),
                T.StructField("title", T.StringType()),
                T.StructField("year", T.StringType()),
                T.StructField("genre", T.ArrayType(T.StringType())),
                T.StructField("style", T.ArrayType(T.StringType())),
                T.StructField("country", T.StringType()),
                T.StructField("format", T.ArrayType(T.StringType())),
            ]))),
        ])

        df_exploded = (
            df_raw
            .withColumn("json", F.from_json("json_data", schema_results))
            .withColumn("release", F.explode("json.results"))
        )

        df_discogs = (
            df_exploded
            .select(
                F.col("release.id").alias("discogs_id"),
                F.col("release.title").alias("discogs_title"),
                F.col("release.year").alias("discogs_year"),
                F.col("release.genre").alias("discogs_genre"),
                F.col("release.style").alias("discogs_style"),
                F.col("release.country").alias("discogs_country"),
                F.col("release.format").alias("discogs_format"),
                F.col("ingest_ts"),
            )
            .dropna(subset=["discogs_title"])
            .withColumn("env", F.lit(config.ENV))
            .withColumn("run_id", F.lit(config.RUN_ID))
        )

        # ✅ Filter for soundtrack/OST-related releases
        df_discogs = df_discogs.filter(
            F.lower(F.coalesce(F.col("discogs_title"), F.lit(""))).rlike("soundtrack|motion picture|score") |
            F.lower(F.concat_ws(" ", F.col("discogs_genre"))).rlike("stage & screen|soundtrack|score|film|movie")
        )

        count_filtered = df_discogs.count()
        self.logger.info(f"🎬 Filtered Discogs releases to OST-only subset: {count_filtered} rows remain")

        # Optional: summary distribution
        summary = (
            df_discogs
            .withColumn("genre_flat", F.explode_outer("discogs_genre"))
            .groupBy(F.lower(F.col("genre_flat")).alias("genre"))
            .count()
            .orderBy(F.desc("count"))
        )
        summary_rows = [f"{r['genre']}: {r['count']}" for r in summary.collect() if r['genre']]
        self.logger.info(f"📊 OST genre breakdown (top 10): {summary_rows[:10]}")

        # (4) Append canonical_id for deterministic lineage
        df_discogs = df_discogs.withColumn(
            "canonical_id",
            F.sha2(
                F.concat_ws("|",
                    F.coalesce(F.col("discogs_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("discogs_title"), F.lit("")),
                    F.coalesce(F.col("discogs_year").cast("string"), F.lit(""))
                ), 256
            )
        )
        self.logger.info("🔗 Appended canonical_id (SHA-256 of discogs_id|title|year).")

        # (5) Persist to Bronze
        self.logger.info(f"💾 Writing to Bronze path: {OUTPUT_PATH}")
        config.write_df(df_discogs, "bronze", "discogs", partition_cols=["env", "run_id"])

        count = df_discogs.count()
        self.logger.info(f"✅ Wrote {count} Discogs records → Bronze ({config.ENV}, run_id={config.RUN_ID})")
        self.logger.info(f"📦 Bronze path: {OUTPUT_PATH}")

        # (6) Metrics
        metrics = {
            "titles_total": count,
            "records_written": count,
            "duration_sec": round(time.time() - t0, 2),
            "output_path": OUTPUT_PATH,
            "page_limit": DISCOGS_PAGE_CAP,
            "env": config.ENV,
            "run_id": config.RUN_ID,
            "step_name": "extract_spark_discogs_metrics",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }

        metrics_path = config.layer_path("metrics", "extract_spark_discogs")
        self.write_metrics(metrics, name="extract_spark_discogs_metrics", metrics_dir=metrics_path)
        self.logger.info(f"📊 Logged Discogs extract metrics to {metrics_path}")
        self.logger.info(f"✅ Step 02 completed successfully in {metrics['duration_sec']} s")

        return df_discogs


# ================================================================
#  Entrypoint for Databricks / Local
# ================================================================
if __name__ == "__main__":
    Step02ExtractSparkDiscogs().run(None)

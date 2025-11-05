# ================================================================
#  extract_spark_discogs.py — UC/ADLS v3.2 (mount-less, config-driven)
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
OUTPUT_PATH = f"{config.INTERMEDIATE_DIR}/discogs"
DISCOGS_API_URL = "https://api.discogs.com/database/search"
DISCOGS_USER_AGENT = config.DISCOGS_USER_AGENT

# ----------------------------------------------------------------
# ⚙️ Pagination & Throttling Controls
# ----------------------------------------------------------------
PAGE_LIMIT        = config.DISCOGS_MAX_TITLES or 1000  # total titles to extract per run
DISCOGS_PER_PAGE  = config.DISCOGS_PER_PAGE            # records returned per Discogs API page
DISCOGS_SLEEP_SEC = config.DISCOGS_SLEEP_SEC           # delay between successive Discogs API calls
DISCOGS_PAGE_CAP  = config.DISCOGS_PAGE_CAP            # per-query page throttle (local)
MAX_PAGINATION_WARN = config.MAX_PAGINATION_WARN       # global pagination guardrail

# ----------------------------------------------------------------
# 🌐 Network Reliability Controls
# ----------------------------------------------------------------
API_TIMEOUT        = config.API_TIMEOUT             # Timeout (seconds) for API request completion
MAX_RETRIES        = config.API_MAX_RETRIES         # Maximum retry attempts per failed request
RETRY_BACKOFF      = config.RETRY_BACKOFF           # Exponential backoff multiplier between retries


# ================================================================
#  Step Definition
# ================================================================
class Step02ExtractSparkDiscogs(BaseStep):
    """Step 02 – Extract Discogs data and store to ADLS (Parquet)."""
    
    def __init__(self):
        super().__init__("step_02_extract_spark_discogs")
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")
        self.metrics_dir = config.METRICS_DIR
        self.logger.info("✅ Initialized Step 02 (config-driven, mount-less)")

    # ------------------------------------------------------------
    def _get_credentials(self):
        """Secret → env fallback for Discogs API credentials."""
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
        """
        Fetch one Discogs page with intelligent retry/back-off and
        pagination safety limits.
        """
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

                # ✅ Success
                if resp.status_code == 200:
                    data = resp.json()
                    pagination = data.get("pagination", {})
                    total_pages = pagination.get("pages", 1)

                    # ⚙️ Bound total pages defensively
                    if total_pages > MAX_PAGINATION_WARN:
                        self.logger.warning(
                            f"⚠️ Pagination reported {total_pages} pages — "
                            f"truncating to {MAX_PAGINATION_WARN} to prevent runaway fetch."
                        )
                        total_pages = MAX_PAGINATION_WARN

                    # 🧭 End-of-range check
                    if page > total_pages:
                        self.logger.info(
                            f"🧭 Page {page} > valid range [1..{total_pages}] "
                            f"for query '{query}'. Halting."
                        )
                        return None

                    return data

                # 🚦 Rate limited
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    sleep_time = float(retry_after) if retry_after else (60 * attempt)
                    self.logger.warning(
                        f"🚦 Rate limited on page {page}. "
                        f"Sleeping {sleep_time:.1f}s before retry {attempt}/{API_MAX_RETRIES}."
                    )
                    time.sleep(sleep_time)
                    continue

                # 🔁 Transient 5xx
                if 500 <= resp.status_code < 600:
                    self.logger.warning(
                        f"Server {resp.status_code} on page {page}. "
                        f"Retrying in {RETRY_BACKOFF:.1f}s ({attempt}/{API_MAX_RETRIES})"
                    )
                    time.sleep(RETRY_BACKOFF * attempt)
                    continue

                # 🛑 Other HTTP codes
                self.logger.error(
                    f"❌ HTTP {resp.status_code} on page {page}: {resp.text[:200]}"
                )
                break

            except requests.RequestException as e:
                self.logger.warning(
                    f"⚠️ Network error on page {page} ({type(e).__name__}): {e}. "
                    f"Retrying in {RETRY_BACKOFF:.1f}s ({attempt}/{API_MAX_RETRIES})"
                )
                time.sleep(RETRY_BACKOFF * attempt)

        self.logger.error(f"❌ Page {page} failed after {API_MAX_RETRIES} retries.")
        return None


    # ------------------------------------------------------------
    def run(self, _: dict | None = None):
        t0 = time.time()
        self.logger.info("🚀 Starting Discogs extraction job")

        key, secret = self._get_credentials()

        # 1️⃣ Download pages
        all_pages = []

        # allow either a string or a list in config
        queries = DISCOGS_QUERY if isinstance(DISCOGS_QUERY, (list, tuple)) else [DISCOGS_QUERY]
        self.logger.info(f"🔍 Using Discogs queries: {queries}")

        # global_cap → upper safety bound (API or config-defined maximum pagination limit)
        global_cap = getattr(self, "max_pagination_warn", PAGE_LIMIT)

        # local_cap → internal safety throttle; constrains how aggressively each query paginates
        local_cap = getattr(self, "safe_page_cap", config.DISCOGS_PAGE_CAP)

        # page_cap → effective working limit; whichever is stricter between global and local caps
        page_cap = min(global_cap, local_cap)

        for term in queries:
            self.logger.info(f"🎧 Starting extraction for term: '{term}' (max {page_cap} pages)")

            for page in range(1, page_cap + 1):
                payload = self._fetch_page(term, key, secret, page)
                if not payload:
                    self.logger.info(f"🧭 No payload for page {page} — stopping pagination for '{term}'.")
                    break

                results = payload.get("results", [])
                if not results:
                    self.logger.info(f"🧭 Page {page} returned 0 results — stopping pagination for '{term}'.")
                    break

                all_pages.append({
                    "query": term,
                    "page": payload.get("pagination", {}).get("page", page),
                    "results": results,
                })
                self.logger.info(f"📥 '{term}' → Page {page} retrieved → {len(results)} results")

                time.sleep(DISCOGS_SLEEP_SEC)

            self.logger.info(f"✅ Completed extraction for term: '{term}' after {page} pages")

        if not all_pages:
            raise RuntimeError("❌ No Discogs data retrieved — check keys or rate limit.")


        # 2️⃣ Convert JSON → Spark DataFrame
        json_rows = [(json.dumps(p),) for p in all_pages]
        json_schema = T.StructType([T.StructField("json_data", T.StringType())])
        df_raw = self.spark.createDataFrame(json_rows, json_schema).withColumn(
            "ingest_ts", F.current_timestamp()
        )

        # 3️⃣ Parse and explode
        schema_results = T.StructType(
            [
                T.StructField("page", T.IntegerType()),
                T.StructField(
                    "results",
                    T.ArrayType(
                        T.StructType(
                            [
                                T.StructField("title", T.StringType()),
                                T.StructField("year", T.StringType()),
                                T.StructField("genre", T.ArrayType(T.StringType())),
                                T.StructField("style", T.ArrayType(T.StringType())),
                                T.StructField("country", T.StringType()),
                                T.StructField("format", T.ArrayType(T.StringType())),
                            ]
                        )
                    ),
                ),
            ]
        )

        df_exploded = (
            df_raw.withColumn("json", F.from_json("json_data", schema_results))
            .withColumn("release", F.explode("json.results"))
        )

        df_discogs = (
            df_exploded.select(
                F.col("release.title").alias("discogs_title"),
                F.col("release.year").alias("discogs_year"),
                F.col("release.genre").alias("discogs_genre"),
                F.col("release.style").alias("discogs_style"),
                F.col("release.country").alias("discogs_country"),
                F.col("release.format").alias("discogs_format"),
                F.col("ingest_ts"),
            ).dropna(subset=["discogs_title"])
        )

        # 4️⃣ Persist to ADLS
        (
            df_discogs.repartition(1)
            .write.mode("overwrite")
            .parquet(OUTPUT_PATH)
        )
        count = df_discogs.count()
        self.logger.info(f"💾 Wrote {count} Discogs records → {OUTPUT_PATH}")

        # 5️⃣ Metrics
        metrics = {
            "titles_total": count,
            "records_written": count,
            "duration_sec": round(time.time() - t0, 2),
            "output_path": OUTPUT_PATH,
            "page_limit": PAGE_LIMIT,
            "query": DISCOGS_QUERY,
        }
        self.write_metrics(metrics, name="extract_spark_discogs_metrics", metrics_dir=self.metrics_dir)
        self.logger.info(f"✅ Completed Discogs extraction in {metrics['duration_sec']} s")

        return df_discogs


# ================================================================
#  Entrypoint for Databricks / Local
# ================================================================
if __name__ == "__main__":
    Step02ExtractSparkDiscogs().run(None)

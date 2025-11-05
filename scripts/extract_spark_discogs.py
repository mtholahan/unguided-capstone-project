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

# ----------------------------------------------------------------
# Let 'er Rip Knobs
# ----------------------------------------------------------------
PAGE_LIMIT = config.DISCOG_MAX_TITLES or 1000   # number of pages to pull or however many pages full-scale
DISCOGS_PER_PAGE = config.DISCOGS_PER_PAGE
DISCOGS_SLEEP_SEC = config.DISCOGS_SLEEP_SEC
DISCOGS_USER_AGENT = config.DISCOGS_USER_AGENT
MAX_PAGINATION_WARN = config.MAX_PAGINATION_WARN

# ----------------------------------------------------------------
# Shared runtime resources
# ----------------------------------------------------------------
spark = config.spark
DISCOGS_API_URL = "https://api.discogs.com/database/search"
QUERY = "soundtrack"     # can be parameterized later

OUTPUT_PATH = f"{config.INTERMEDIATE_DIR}/discogs"

API_TIMEOUT = config.API_TIMEOUT
API_MAX_RETRIES = config.API_MAX_RETRIES
RETRY_BACKOFF = config.RETRY_BACKOFF

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

        # prefer the API’s own defensive cap if you have it in config, else fall back
        max_pages = getattr(self, "MAX_PAGINATION_WARN", PAGE_LIMIT)

        for page in range(1, max_pages + 1):
            # _fetch_page(query, key, secret, page) → dict | None
            payload = self._fetch_page(query, key, secret, page)
            if not payload:
                self.logger.info(f"🧭 No payload for page {page} — stopping pagination.")
                break

            results = payload.get("results", [])
            if not results:
                self.logger.info(f"🧭 Page {page} returned 0 results — stopping pagination.")
                break

            all_pages.append(
                {
                    "page": payload.get("pagination", {}).get("page", page),
                    "results": results,
                }
            )
            self.logger.info(f"📥 Page {page} retrieved → {len(results)} results")

            time.sleep(DISCOGS_SLEEP_SEC)

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
            "query": QUERY,
        }
        self.write_metrics(metrics, name="extract_spark_discogs_metrics", metrics_dir=self.metrics_dir)
        self.logger.info(f"✅ Completed Discogs extraction in {metrics['duration_sec']} s")

        return df_discogs


# ================================================================
#  Entrypoint for Databricks / Local
# ================================================================
if __name__ == "__main__":
    Step02ExtractSparkDiscogs().run(None)

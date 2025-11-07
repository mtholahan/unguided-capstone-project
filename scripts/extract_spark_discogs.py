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
# Shared runtime resources
# ----------------------------------------------------------------
spark = config.spark
DISCOGS_API_URL = "https://api.discogs.com/database/search"
PAGE_LIMIT = 5           # number of pages to pull (50 per page)
QUERY = "soundtrack"     # can be parameterized later

OUTPUT_PATH = f"{config.INTERMEDIATE_DIR}/discogs"

API_TIMEOUT = config.API_TIMEOUT
MAX_RETRIES = config.API_MAX_RETRIES
RETRY_BACKOFF = config.RETRY_BACKOFF
REQUEST_DELAY = config.DISCOGS_SLEEP_SEC


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
    def _fetch_page(self, key, secret, page):
        """Fetch one Discogs page with retry/back-off."""
        params = {
            "q": QUERY,
            "type": "release",
            "per_page": 50,
            "page": page,
            "key": key,
            "secret": secret,
        }
        attempts = 0
        while attempts <= MAX_RETRIES:
            resp = requests.get(DISCOGS_API_URL, params=params, timeout=API_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            self.logger.warning(
                f"Discogs page {page} failed ({resp.status_code}); retry {attempts+1}/{MAX_RETRIES}"
            )
            attempts += 1
            time.sleep(RETRY_BACKOFF)
        self.logger.error(f"❌ Discogs page {page} failed after {MAX_RETRIES} retries.")
        return None

    # ------------------------------------------------------------
    def run(self, _: dict | None = None):
        t0 = time.time()
        self.logger.info("🚀 Starting Discogs extraction job")

        key, secret = self._get_credentials()

        # 1️⃣ Download pages
        all_pages = []
        for page in range(1, PAGE_LIMIT + 1):
            payload = self._fetch_page(key, secret, page)
            if payload:
                results = payload.get("results", [])
                all_pages.append(
                    {"page": payload.get("pagination", {}).get("page", page), "results": results}
                )
                self.logger.info(f"📥 Page {page} retrieved → {len(results)} results")
            time.sleep(REQUEST_DELAY)

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

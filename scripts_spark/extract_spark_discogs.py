# ================================================================
#  extract_spark_discogs.py — Refactored for Databricks + ADLS Gen2
# ================================================================

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.dbutils import DBUtils
import time
import json
import requests
from scripts.base_step import BaseStep


# ================================================================
#  Initialize Spark + Secrets
# ================================================================
spark = SparkSession.builder.appName("Step02_ExtractSparkDiscogs").getOrCreate()
dbutils = DBUtils(spark)

STORAGE_ACCOUNT = dbutils.secrets.get("markscope", "azure-storage-account-name").strip()
STORAGE_KEY = dbutils.secrets.get("markscope", "azure-storage-account-key").strip()

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY,
)

# ================================================================
#  Constants
# ================================================================
BASE_URI = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
OUTPUT_PATH = f"{BASE_URI}/raw/discogs/"
DISCOGS_API_URL = "https://api.discogs.com/database/search"

PAGE_LIMIT = 5      # adjust as needed (50 per page × N pages)
QUERY = "soundtrack"  # can change or parametrize


# ================================================================
#  Step Definition
# ================================================================
class Step02ExtractSparkDiscogs(BaseStep):
    """Step 02 – Extract Discogs data and store to ADLS (Parquet)."""

    def __init__(self, spark: SparkSession):
        super().__init__("step_02_extract_spark_discogs")
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("✅ Initialized Step 02: Extract Spark Discogs")

    # ------------------------------------------------------------
    def run(self, config: dict | None = None):
        t0 = time.time()
        self.logger.info("🚀 Starting Discogs Spark extraction")

        # --------------------------------------------------------
        # 1️⃣ Retrieve API credentials
        # --------------------------------------------------------
        try:
            consumer_key = dbutils.secrets.get("markscope", "discogs-consumer-key").strip()
            consumer_secret = dbutils.secrets.get("markscope", "discogs-consumer-secret").strip()
        except Exception:
            import os
            consumer_key = os.getenv("DISCOGS_CONSUMER_KEY")
            consumer_secret = os.getenv("DISCOGS_CONSUMER_SECRET")

        if not (consumer_key and consumer_secret):
            raise ValueError("❌ Discogs API credentials not found in secrets or environment.")

        # --------------------------------------------------------
        # 2️⃣ Make authenticated API requests (token-based)
        # --------------------------------------------------------
        all_pages = []
        params = {
            "q": QUERY,
            "type": "release",
            "per_page": 50,
            "key": consumer_key,
            "secret": consumer_secret,
        }

        for page in range(1, PAGE_LIMIT + 1):
            params["page"] = page
            print(f"🔍 Requesting page {page} → {DISCOGS_API_URL}")
            resp = requests.get(DISCOGS_API_URL, params=params, timeout=30)

            if resp.status_code != 200:
                self.logger.warning(f"⚠️ Discogs page {page} failed: {resp.text[:200]}")
                continue

            payload = resp.json()
            all_pages.append({
                "page": payload.get("pagination", {}).get("page", page),
                "results": payload.get("results", []),
            })
            self.logger.info(f"📥 Page {page} retrieved: {len(payload.get('results', []))} results")

        if not all_pages:
            raise RuntimeError("❌ No Discogs pages retrieved — check API keys or rate limit.")

        # --------------------------------------------------------
        # 3️⃣ Convert JSON pages into Spark DataFrame
        # --------------------------------------------------------
        json_rows = [(json.dumps(page),) for page in all_pages]
        json_schema = T.StructType([T.StructField("json_data", T.StringType())])
        df_raw = self.spark.createDataFrame(json_rows, json_schema)
        df_raw = df_raw.withColumn("timestamp", F.current_timestamp())

        # --------------------------------------------------------
        # 4️⃣ Parse json_data and explode "results" array
        # --------------------------------------------------------
        schema_results = T.StructType([
            T.StructField("page", T.IntegerType()),
            T.StructField("results", T.ArrayType(
                T.StructType([
                    T.StructField("title", T.StringType()),
                    T.StructField("year", T.StringType()),
                    T.StructField("genre", T.ArrayType(T.StringType())),
                    T.StructField("style", T.ArrayType(T.StringType())),
                    T.StructField("country", T.StringType()),
                    T.StructField("format", T.ArrayType(T.StringType())),
                ])
            ))
        ])

        df_exploded = (
            df_raw
            .withColumn("json", F.from_json("json_data", schema_results))
            .withColumn("release", F.explode("json.results"))
        )

        df_discogs = (
            df_exploded
            .select(
                F.col("release.title").alias("discogs_title"),
                F.col("release.year").alias("discogs_year"),
                F.col("release.genre").alias("discogs_genre"),
                F.col("release.style").alias("discogs_style"),
                F.col("release.country").alias("discogs_country"),
                F.col("release.format").alias("discogs_format"),
            )
            .dropna(subset=["discogs_title"])
        )

        # --------------------------------------------------------
        # 5️⃣ Persist to ADLS
        # --------------------------------------------------------
        df_discogs.write.mode("overwrite").parquet(OUTPUT_PATH)
        count = df_discogs.count()
        self.logger.info(f"💾 Wrote {count} Discogs records → {OUTPUT_PATH}")

        # --------------------------------------------------------
        # 6️⃣ Record metrics
        # --------------------------------------------------------
        metrics = {
            "titles_total": count,
            "records_written": count,
            "duration_sec": round(time.time() - t0, 2),
            "output_path": OUTPUT_PATH,
        }
        self.write_metrics(metrics, name="extract_spark_discogs_metrics")
        self.logger.info(f"✅ Completed Discogs Spark extraction in {metrics['duration_sec']} s")

        return df_discogs

    # ------------------------------------------------------------
    @staticmethod
    def run_step(spark: SparkSession, config: dict | None = None):
        return Step02ExtractSparkDiscogs(spark).run(config)


# ================================================================
#  Entrypoint for Databricks / Local
# ================================================================
if __name__ == "__main__":
    Step02ExtractSparkDiscogs.run_step(spark)
    spark.stop()

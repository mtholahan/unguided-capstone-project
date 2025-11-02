# ================================================================
#  extract_spark_tmdb.py  — Refactored for Databricks + ADLS Gen2
# ================================================================

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.dbutils import DBUtils
import time
import json
import requests
from scripts.base_step import BaseStep


# ================================================================
#  Initialize Spark and Secrets
# ================================================================
spark = SparkSession.builder.appName("Step01_ExtractSparkTMDB").getOrCreate()
dbutils = DBUtils(spark)

# Retrieve storage account info dynamically from Databricks secret scope
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
OUTPUT_PATH = f"{BASE_URI}/raw/tmdb/"
TMDB_API_URL = "https://api.themoviedb.org/3/movie/popular"
PAGE_LIMIT = 5   # adjust as needed (5 pages × 20 results = 100 movies)


# ================================================================
#  Step Definition
# ================================================================
class Step01ExtractSparkTMDB(BaseStep):
    """Step 01 – Extract TMDB data and store to ADLS (Parquet)."""

    def __init__(self, spark: SparkSession):
        super().__init__("step_01_extract_spark_tmdb")
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("✅ Initialized Step 01: Extract Spark TMDB")

    # ------------------------------------------------------------
    def run(self, config: dict | None = None):
        t0 = time.time()
        self.logger.info("🚀 Starting TMDB extraction job")

        # --------------------------------------------------------
        # 1️⃣ Retrieve TMDB API key from Databricks secrets
        # --------------------------------------------------------
        try:
            api_key = dbutils.secrets.get("markscope", "tmdb-api-key").strip()
        except Exception:
            import os
            api_key = os.getenv("TMDB_API_KEY")
        if not api_key:
            raise ValueError("❌ TMDB API key not found in secrets or environment variables.")

        # --------------------------------------------------------
        # 2️⃣ Download TMDB pages (JSON)
        # --------------------------------------------------------
        all_pages = []
        for page in range(1, PAGE_LIMIT + 1):
            url = f"{TMDB_API_URL}?api_key={api_key}&language=en-US&page={page}"
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                self.logger.warning(f"⚠️ TMDB request failed for page {page}: {resp.text[:200]}")
                continue
            payload = resp.json()
            all_pages.append({
                "page": payload.get("page"),
                "results": payload.get("results", []),
            })
            self.logger.info(f"📥 Page {page} retrieved: {len(payload.get('results', []))} results")

        if not all_pages:
            raise RuntimeError("❌ No TMDB data retrieved — check API key or network connectivity.")

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
                    T.StructField("release_date", T.StringType()),
                    T.StructField("genre_ids", T.ArrayType(T.IntegerType()))
                ])
            ))
        ])

        df_exploded = (
            df_raw
            .withColumn("json", F.from_json("json_data", schema_results))
            .withColumn("movie", F.explode("json.results"))
        )

        df_tmdb = (
            df_exploded
            .select(
                F.col("movie.title").alias("tmdb_title"),
                F.col("movie.release_date").alias("tmdb_release_date"),
                F.col("movie.genre_ids").alias("tmdb_genre_ids"),
            )
            .withColumn("tmdb_year", F.substring(F.col("tmdb_release_date"), 1, 4).cast("int"))
            .dropna(subset=["tmdb_title"])
        )

        # --------------------------------------------------------
        # 5️⃣ Persist to ADLS
        # --------------------------------------------------------
        df_tmdb.write.mode("overwrite").parquet(OUTPUT_PATH)
        count = df_tmdb.count()
        self.logger.info(f"💾 Wrote {count} TMDB records → {OUTPUT_PATH}")

        # --------------------------------------------------------
        # 6️⃣ Record metrics
        # --------------------------------------------------------
        metrics = {
            "titles_total": count,
            "records_written": count,
            "duration_sec": round(time.time() - t0, 2),
            "output_path": OUTPUT_PATH,
        }
        self.write_metrics(metrics, name="extract_spark_tmdb_metrics")
        self.logger.info(f"✅ Completed TMDB Spark extraction in {metrics['duration_sec']} s")

        return df_tmdb

    # ------------------------------------------------------------
    @staticmethod
    def run_step(spark: SparkSession, config: dict | None = None):
        return Step01ExtractSparkTMDB(spark).run(config)


# ================================================================
#  Entrypoint for Databricks / Local
# ================================================================
if __name__ == "__main__":
    Step01ExtractSparkTMDB.run_step(spark)
    spark.stop()

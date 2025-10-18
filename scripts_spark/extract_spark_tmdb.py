"""
extract_spark_tmdb.py
Step 01 (PySpark Refactor): Acquire TMDB Metadata
Unguided Capstone Project – TMDB→Discogs Directional Refactor (Step 6.3)

Refactored for Databricks:
  - Uses Spark for distributed persistence (Blob writes)
  - Reads from /dbfs/ or abfss:// paths
  - Pulls secrets from Databricks scope: capstone-secrets
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import requests, json, time
from pathlib import Path
from datetime import datetime

# --- Local project imports ---
from scripts.base_step import BaseStep
from scripts.config import USE_GOLDEN_LIST, GOLDEN_TITLES_TEST
from scripts.utils import safe_filename

class ExtractSparkTMDB(BaseStep):
    """Fetch TMDB metadata in Spark context and write JSON to Blob Storage."""

    def __init__(self, spark: SparkSession):
        super().__init__(name="extract_spark_tmdb")
        self.spark = spark

        # Pull TMDB API key from Databricks secret scope
        try:
            import dbruntime.dbutils as dbutils  # runtime safe import
        except ImportError:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)

        self.tmdb_api_key = dbutils.secrets.get(scope="capstone-secrets", key="tmdb_api_key")

        # Azure Blob path (OAuth-authenticated)
        self.container_uri = "abfss://capstone-data@markcapstonestorage.dfs.core.windows.net/raw/tmdb/"
        self.rate_limit = 3  # ~3 req/sec
        self.logger.info("✅ Initialized ExtractSparkTMDB with Spark + Blob access")

    # ------------------------------------------------------------------
    def _fetch_tmdb(self, title: str):
        """Perform TMDB API request for one title."""
        url = "https://api.themoviedb.org/3/search/movie"
        params = {"query": title, "api_key": self.tmdb_api_key}
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        else:
            self.logger.warning(f"⚠️ TMDB fetch failed for '{title}' ({resp.status_code})")
            return None

    # ------------------------------------------------------------------
    def run(self):
        """Main Spark entrypoint."""
        start_time = time.time()

        # --- Determine titles ---
        if USE_GOLDEN_LIST:
            titles = GOLDEN_TITLES_TEST
            mode = "GOLDEN"
        else:
            titles_path = Path("/dbfs/FileStore/capstone-data/movie_titles_200.txt")
            titles = [t.strip() for t in titles_path.read_text().splitlines() if t.strip()]
            mode = f"AUTO ({len(titles)} titles)"

        self.logger.info(f"🎬 Starting Spark TMDB extraction ({mode})")

        results = []
        for i, title in enumerate(titles, start=1):
            data = self._fetch_tmdb(title)
            if data:
                results.append({"title": title, "json_data": json.dumps(data)})
            time.sleep(1 / self.rate_limit)

        # --- Convert to Spark DataFrame ---
        df = self.spark.createDataFrame(results)
        df = df.withColumn("timestamp", lit(datetime.utcnow().isoformat()))

        # --- Write to Azure Blob (Parquet format) ---
        df.write.mode("overwrite").parquet(self.container_uri)
        self.logger.info(f"💾 Wrote {df.count()} TMDB JSON records to {self.container_uri}")

        duration = round(time.time() - start_time, 2)
        self.write_metrics(
            {
                "titles_total": len(titles),
                "records_written": df.count(),
                "duration_sec": duration,
                "mode": mode,
                "branch": "step6-dev",
            },
            name="extract_spark_tmdb_metrics"
        )

        self.logger.info(f"✅ Completed Spark TMDB extraction in {duration:.1f}s")

# ----------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("CapstoneExtractTMDB").getOrCreate()
    ExtractSparkTMDB(spark).run()

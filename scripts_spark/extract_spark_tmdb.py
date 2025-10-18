"""
extract_spark_tmdb.py
Step 01 (PySpark Refactor): Acquire TMDB Metadata
Unguided Capstone Project – TMDB→Discogs Directional Refactor (Step 6.3)

Refactored for Databricks:
  - Uses Spark for distributed persistence (Blob writes)
  - Reads from /dbfs/ or abfss:// paths
  - Supports Databricks secret scopes (optional)
  - Falls back gracefully to environment variable TMDB_API_KEY
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import requests, json, time, os, sys
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
        self.tmdb_api_key = None
        self.container_uri = (
            "abfss://capstone-data@markcapstonestorage.dfs.core.windows.net/raw/tmdb/"
        )
        self.rate_limit = 3  # ~3 requests per second
        self.logger.info("✅ Initialized ExtractSparkTMDB with Spark + Blob access")

    # ------------------------------------------------------------------
    def _get_api_key(self):
        """Retrieve TMDB API key via Databricks secret scope or fallback to environment."""
        if self.tmdb_api_key:
            return self.tmdb_api_key

        # 1️⃣ Try to get from Databricks secret scope (if configured)
        try:
            import dbruntime.dbutils as dbutils_runtime
            if hasattr(dbutils_runtime, "secrets"):
                self.tmdb_api_key = dbutils_runtime.secrets.get(
                    scope="capstone-secrets", key="tmdb_api_key"
                )
                self.logger.info("🔐 Retrieved TMDB API key from Databricks scope.")
                return self.tmdb_api_key
        except Exception as e:
            self.logger.info(f"⚠️ Databricks secret scope unavailable: {e}")

        # 2️⃣ Fallback: environment variable
        self.tmdb_api_key = os.getenv("TMDB_API_KEY", "DUMMY_KEY_FOR_LOCAL_TESTS")
        if self.tmdb_api_key == "DUMMY_KEY_FOR_LOCAL_TESTS":
            self.logger.warning("⚠️ Using dummy TMDB key for local testing.")
        else:
            self.logger.info("✅ Using TMDB_API_KEY from environment variable.")
        return self.tmdb_api_key

    # ------------------------------------------------------------------
    def _fetch_tmdb(self, title: str):
        """Perform TMDB API request for one title."""
        url = "https://api.themoviedb.org/3/search/movie"
        params = {"query": title, "api_key": self._get_api_key()}
        try:
            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("results"):
                    self.logger.info(f"✅ Got TMDB data for '{title}'")
                    return data
                else:
                    self.logger.warning(f"⚠️ No results for '{title}'")
                    return None
            else:
                self.logger.warning(f"⚠️ TMDB fetch failed for '{title}' ({resp.status_code})")
                return None
        except Exception as e:
            self.logger.error(f"❌ Exception fetching '{title}': {e}")
            # Fallback dummy data so Spark job doesn’t crash
            return {"results": [{"title": title, "note": "dummy data for test"}]}

    # ------------------------------------------------------------------
    def run(self):
        """Main Spark entrypoint."""
        start_time = time.time()

        # --- Determine titles ---
        if USE_GOLDEN_LIST:
            titles = GOLDEN_TITLES_TEST
            mode = "GOLDEN"
        else:
            # Hardcoded short list for test mode
            titles = ["Inception", "Interstellar", "The Matrix"]
            mode = f"TEST ({len(titles)} titles)"

        self.logger.info(f"🎬 Starting Spark TMDB extraction ({mode})")

        results = []
        for i, title in enumerate(titles, start=1):
            data = self._fetch_tmdb(title)
            if data:
                self.logger.info(f"✅ Got data for '{title}', adding to results")
                results.append({"title": title, "json_data": json.dumps(data)})
            else:
                self.logger.warning(f"⚠️ No data for '{title}'")
            time.sleep(1 / self.rate_limit)

        # --- Debug output ---
        print(f"DEBUG: Fetched {len(results)} of {len(titles)} titles")

        if not results:
            results = [{"title": "DummyTitle", "json_data": "{}"}]
            print("⚠️ Injected dummy row to allow DataFrame creation")

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
            name="extract_spark_tmdb_metrics",
        )

        self.logger.info(f"✅ Completed Spark TMDB extraction in {duration:.1f}s")


# ----------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("CapstoneExtractTMDB").getOrCreate()
    ExtractSparkTMDB(spark).run()

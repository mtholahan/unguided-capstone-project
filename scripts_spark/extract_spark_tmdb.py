"""
extract_spark_tmdb.py
Step 01 (PySpark Refactor): Acquire TMDB Metadata
Unguided Capstone Project – Step 8 (Deploy for Testing)

Databricks refactor:
  • Uses Databricks Secrets Scope (`markscope`) for credentials
  • Handles LOCAL_MODE for offline testing
  • Writes results to ABFSS (Azure) or DBFS (Databricks)
  • Structured logging, metrics, and Spark-safe error handling
"""

import os, re, json, time, requests
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# --- Local imports ---
from scripts.base_step import BaseStep
from scripts.config import USE_GOLDEN_LIST, GOLDEN_TITLES_TEST
from scripts.utils import safe_filename


class ExtractSparkTMDB(BaseStep):
    """Fetch TMDB metadata using Spark context."""

    def __init__(self, spark: SparkSession, local_mode: bool = False):
        super().__init__(name="step_01_extract_spark_tmdb")
        self.spark = spark
        self.local_mode = local_mode
        self.rate_limit = 3  # ~3 requests/sec
        self.tmdb_api_key = None

        # Try to configure Azure ABFS output
        try:
            if not self.local_mode:
                self.container_uri = "abfss://raw@ungcaptor01.dfs.core.windows.net/raw/tmdb/"
                self.logger.info("✅ Configured Spark for Azure ABFS output.")
            else:
                raise ValueError("Forced local mode.")
        except Exception as e:
            self.logger.warning(f"⚠️ Falling back to LOCAL_MODE → {e}")
            self.local_mode = True
            self.container_uri = "dbfs:/tmp/tmdb_output/"
            os.makedirs("/dbfs/tmp/tmdb_output/", exist_ok=True)

        self.logger.info(f"🧩 Running mode: {'LOCAL' if self.local_mode else 'AZURE'}")

    # ------------------------------------------------------------------
    def _get_api_key(self):
        """Retrieve TMDB API key from Databricks Secrets Scope."""
        if self.tmdb_api_key:
            return self.tmdb_api_key

        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            self.tmdb_api_key = dbutils.secrets.get("markscope", "tmdb-api-key")
            assert self.tmdb_api_key, "TMDB API key is empty or missing"
            self.logger.info("🔐 Retrieved TMDB API key from Databricks secrets.")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not read from Databricks secrets → {e}")
            self.tmdb_api_key = os.getenv("TMDB_API_KEY", "DUMMY_KEY_FOR_LOCAL_TESTS")
            self.logger.info("Using fallback TMDB_API_KEY from environment.")
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
                    self.logger.debug(f"✅ TMDB data fetched for '{title}'")
                    return data
                self.logger.info(f"⚠️ No results for '{title}'")
                return None
            self.logger.warning(f"⚠️ TMDB fetch failed for '{title}' ({resp.status_code})")
            return None
        except Exception as e:
            self.logger.error(f"❌ Exception fetching '{title}': {e}")
            return {"results": [{"title": title, "note": "dummy data"}]}

    # ------------------------------------------------------------------
    def run(self):
        """Main Spark entrypoint."""
        start_time = time.time()
        mode = "GOLDEN" if USE_GOLDEN_LIST else "TEST"

        if USE_GOLDEN_LIST:
            if isinstance(GOLDEN_TITLES_TEST, str):
                raw_titles = re.split(r"[,;]+|\s{2,}", GOLDEN_TITLES_TEST)
                titles = [t.strip() for t in raw_titles if t.strip()]
            else:
                titles = list(GOLDEN_TITLES_TEST)
        else:
            titles = ["Inception", "Interstellar", "The Matrix"]

        self.logger.info(f"🎞️ Starting TMDB extraction ({mode}, {len(titles)} titles)")

        results = []
        for title in titles:
            data = self._fetch_tmdb(title)
            if data:
                results.append({"title": title, "json_data": json.dumps(data)})
            time.sleep(1 / self.rate_limit)

        if not results:
            self.logger.warning("⚠️ No TMDB data fetched — inserting dummy record.")
            results = [{"title": "DummyTitle", "json_data": "{}"}]

        df = (
            self.spark.createDataFrame(results)
            .withColumn("timestamp", lit(datetime.now(timezone.utc).isoformat()))
        )

        write_path = self.container_uri
        df.write.mode("overwrite").parquet(write_path)
        count = df.count()
        self.logger.info(f"💾 Wrote {count} TMDB records → {write_path}")

        self.write_metrics(
            {
                "titles_total": len(titles),
                "records_written": count,
                "duration_sec": round(time.time() - start_time, 2),
                "mode": mode,
                "local_mode": self.local_mode,
                "branch": "step8-dbx",
            },
            name="extract_spark_tmdb_metrics",
        )

        self.logger.info("✅ Completed TMDB Spark extraction")


# ----------------------------------------------------------------------
# Databricks entrypoint
if __name__ == "__main__":
    spark = SparkSession.builder.appName("ExtractSparkTMDB").getOrCreate()
    job = ExtractSparkTMDB(spark, local_mode=False)
    job.run()

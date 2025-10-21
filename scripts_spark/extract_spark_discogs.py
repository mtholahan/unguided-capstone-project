"""
extract_spark_discogs.py
Step 02 (PySpark Refactor): Acquire Discogs Metadata
Unguided Capstone Project – TMDB→Discogs Directional Refactor (Step 6.3)

Refactored for Databricks:
  - Uses Spark for distributed persistence (Blob writes)
  - Reads TMDB titles from /raw/tmdb parquet (Step 01 output)
  - Queries Discogs API with environment/secret fallback
  - Persists results to abfss://raw/discogs/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import requests, json, os, time
from datetime import datetime

# --- Local project imports ---
from scripts.base_step import BaseStep
from scripts.utils import safe_filename


class ExtractSparkDiscogs(BaseStep):
    """Fetch Discogs metadata in Spark context and write JSON to Blob Storage."""

    def __init__(self, spark: SparkSession):
        super().__init__(name="extract_spark_discogs")
        self.spark = spark
        self.api_url = "https://api.discogs.com/database/search"
        self.container_uri = (
            "abfss://raw@markcapstoneadls.dfs.core.windows.net/raw/discogs/"
        )
        self.rate_limit = 2  # ~2 requests per second
        self.logger.info("✅ Initialized ExtractSparkDiscogs with Spark + Blob access")

    # ------------------------------------------------------------------
    def _get_api_key(self):
        """Retrieve Discogs API key from Databricks secret scope or environment."""
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            key = dbutils.secrets.get("markscope", "discogs-api-key")
            self.logger.info("🔐 Retrieved Discogs API key from Databricks secret scope.")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load Discogs secret from scope: {e}")
            key = os.getenv("DISCOGS_API_KEY", "DUMMY_KEY_FOR_LOCAL_TESTS")
            if key == "DUMMY_KEY_FOR_LOCAL_TESTS":
                self.logger.warning("⚠️ Using dummy Discogs key for local testing.")
            else:
                self.logger.info("✅ Using DISCOGS_API_KEY from environment variable.")

        return key

    # ------------------------------------------------------------------
    def _fetch_discogs(self, title: str, api_key: str):
        """Perform Discogs API request for one title."""
        params = {
            "q": f"{title} soundtrack",
            "type": "release",
            "per_page": 5,
            "page": 1,
            "token": api_key,
        }
        headers = {"User-Agent": "CapstoneSparkDiscogs/1.0"}
        try:
            resp = requests.get(self.api_url, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                filtered = [
                    r
                    for r in data.get("results", [])
                    if any(k in r.get("title", "").lower() for k in ["soundtrack", "ost", "score"])
                ]
                self.logger.info(f"✅ Discogs results for '{title}': {len(filtered)} hits")
                return {"title": title, "results": filtered}
            else:
                self.logger.warning(f"⚠️ Discogs fetch failed for '{title}' ({resp.status_code})")
                return None
        except Exception as e:
            self.logger.error(f"❌ Exception fetching '{title}': {e}")
            return {"title": title, "results": []}

    # ------------------------------------------------------------------
    def run(self):
        """Main Spark entrypoint."""
        start_time = time.time()
        api_key = self._get_api_key()

        # --- Load TMDB titles from previous step (correct ADLS location) ---
        base_uri = "abfss://raw@markcapstoneadls.dfs.core.windows.net/"
        tmdb_path = f"{base_uri}tmdb/"

        try:
            tmdb_df = self.spark.read.parquet(tmdb_path)
            titles = [row.title for row in tmdb_df.select("title").distinct().collect()]
            self.logger.info(f"✅ Loaded {len(titles)} TMDB titles from ADLS for Discogs lookup.")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not read TMDB data from {tmdb_path}: {e}")
            titles = ["Inception", "Interstellar", "The Matrix"]  # fallback

        # --- Update output container URI ---
        self.container_uri = f"{base_uri}discogs/"

        results = []
        for i, title in enumerate(titles, start=1):
            data = self._fetch_discogs(title, api_key)
            if data:
                results.append({"title": title, "json_data": json.dumps(data)})
            time.sleep(1 / self.rate_limit)
            if i % 25 == 0 or i == len(titles):
                self.logger.info(f"Progress: {i}/{len(titles)} titles processed")

        if not results:
            results = [{"title": "DummyTitle", "json_data": "{}"}]
            self.logger.warning("⚠️ No data returned — injected dummy row to keep schema consistent.")

        # --- Convert to Spark DataFrame ---
        df = self.spark.createDataFrame(results)
        df = df.withColumn("timestamp", lit(datetime.utcnow().isoformat()))

        # --- Write to Azure Blob ---
        df.write.mode("overwrite").parquet(self.container_uri)
        self.logger.info(f"💾 Wrote {df.count()} Discogs JSON records to {self.container_uri}")

        # --- Metrics ---
        duration = round(time.time() - start_time, 2)
        self.write_metrics(
            {
                "titles_total": len(titles),
                "records_written": df.count(),
                "duration_sec": duration,
                "branch": "step6-dev",
            },
            name="extract_spark_discogs_metrics",
        )

        self.logger.info(f"✅ Completed Spark Discogs extraction in {duration:.1f}s")


# ----------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("CapstoneExtractDiscogs").getOrCreate()
    ExtractSparkDiscogs(spark).run()

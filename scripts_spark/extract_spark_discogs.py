"""
extract_spark_discogs.py
Step 02 (PySpark Refactor): Acquire Discogs Metadata
Unguided Capstone Project – TMDB→Discogs Directional Refactor (Step 6.3)

Refactored for Databricks:
  - Reads TMDB titles from /raw/tmdb parquet
  - Queries Discogs API (secure key via Databricks secret or env var)
  - Persists JSON results to abfss://raw/discogs/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import requests, json, os, time
from datetime import datetime
from scripts.base_step import BaseStep


class ExtractSparkDiscogs(BaseStep):
    """Fetch Discogs metadata via Spark and persist JSON to ADLS."""

    def __init__(self, spark: SparkSession, local_mode: bool = False):
        super().__init__(name="extract_spark_discogs")
        self.spark = spark
        self.local_mode = local_mode
        self.rate_limit = 2  # requests/sec
        self.api_url = "https://api.discogs.com/database/search"
        self.consumer_key, self.consumer_secret = self._get_api_creds()

        self.account = "ungcapstor01"
        self.container_uri = (
            f"abfss://raw@{self.account}.dfs.core.windows.net/raw/discogs/"
            if not local_mode else "dbfs:/tmp/discogs_output/"
        )

        self.logger.info(f"💡 Running mode: {'AZURE' if not local_mode else 'LOCAL'}")

    # ------------------------------------------------------------------
    def _get_api_creds(self):
        """Retrieve Discogs consumer key/secret from secret scope or environment."""
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            key = dbutils.secrets.get("markscope", "discogs-consumer-key")
            secret = dbutils.secrets.get("markscope", "discogs-consumer-secret")
            self.logger.info("🔐 Retrieved Discogs consumer key/secret from Databricks secrets.")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load Discogs credentials from scope: {e}")
            key = os.getenv("DISCOGS_CONSUMER_KEY", "dummy_key")
            secret = os.getenv("DISCOGS_CONSUMER_SECRET", "dummy_secret")
        return key, secret

    # ------------------------------------------------------------------
    def _fetch_discogs(self, title: str):
        """Fetch metadata for one title via Discogs API using key/secret pair."""
        params = {
            "q": f"{title} soundtrack",
            "type": "release",
            "per_page": 5,
            "page": 1,
            "key": self.consumer_key,
            "secret": self.consumer_secret,
        }
        headers = {"User-Agent": "DataEngineeringCapstone/1.0"}

        try:
            resp = requests.get(self.api_url, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                results = [
                    r for r in data.get("results", [])
                    if any(k in r.get("title", "").lower() for k in ["soundtrack", "ost", "score"])
                ]
                self.logger.info(f"✅ Discogs results for '{title}': {len(results)} hits")
                return {"title": title, "results": results}
            else:
                self.logger.warning(f"⚠️ Discogs fetch failed for '{title}' ({resp.status_code})")
                return {"title": title, "results": []}
        except Exception as e:
            self.logger.error(f"❌ Exception fetching '{title}': {e}")
            return {"title": title, "results": []}


    # ------------------------------------------------------------------
    def run(self):
        start = time.time()
        base_uri = f"abfss://raw@{self.account}.dfs.core.windows.net/raw/tmdb/"
        try:
            tmdb_df = self.spark.read.parquet(base_uri)
            titles = [r.title for r in tmdb_df.select("title").distinct().collect()]
            self.logger.info(f"📦 Loaded {len(titles)} TMDB titles.")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to load TMDB titles: {e}")
            titles = ["Inception", "Interstellar", "The Matrix"]

        results = [self._fetch_discogs(t) for t in titles]
        time.sleep(len(titles) / self.rate_limit)

        df = self.spark.createDataFrame(
            [{"title": r["title"], "json_data": json.dumps(r["results"])} for r in results]
        ).withColumn("timestamp", lit(datetime.utcnow().isoformat()))

        df.write.mode("overwrite").parquet(self.container_uri)
        count = df.count()
        self.logger.info(f"💾 Wrote {count} Discogs records → {self.container_uri}")

        self.write_metrics(
            {"titles_total": len(titles), "records_written": count, "duration_sec": round(time.time() - start, 2)},
            name="extract_spark_discogs_metrics",
        )

        self.logger.info(f"✅ Completed Discogs extraction in {time.time() - start:.1f}s")


# ----------------------------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("CapstoneExtractDiscogs").getOrCreate()
    ExtractSparkDiscogs(spark).run()

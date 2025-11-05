# ================================================================
#  extract_spark_tmdb.py — UC/ADLS v3.2 (mount-less, config-driven)
#  ---------------------------------------------------------------
#  Purpose : Extract popular TMDB movies and write Parquet to ADLS
#  Runtime : Databricks 16.4 LTS (Spark-based)
#  Author  : M. Holahan
# ================================================================

import scripts.bootstrap  # Ensures package discovery (local + Databricks)
from scripts import config
from scripts.base_step import BaseStep
from pyspark.sql import functions as F, types as T
import os, time, json, requests


# ----------------------------------------------------------------
# Shared runtime resources
# ----------------------------------------------------------------
spark = config.spark

OUTPUT_PATH = f"{config.INTERMEDIATE_DIR}/tmdb"

TMDB_API_URL = "https://api.themoviedb.org/3/movie/popular"
PAGE_LIMIT   = config.TMDB_MAX_RESULTS or 1000
API_TIMEOUT  = config.API_TIMEOUT
MAX_RETRIES  = config.API_MAX_RETRIES
RETRY_BACKOFF = config.RETRY_BACKOFF
REQUEST_DELAY = config.TMDB_SLEEP_SEC


# ================================================================
#  Extract TMDB Step
# ================================================================
class Step01ExtractSparkTMDB(BaseStep):
    """Step 01 – Extract TMDB data and store to ADLS (Parquet)."""

    def __init__(self):
        super().__init__("step_01_extract_spark_tmdb")
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")
        self.metrics_dir = config.METRICS_DIR
        self.logger.info("✅ Initialized Step 01 (config-driven, mount-less)")

    # ------------------------------------------------------------
    def _get_tmdb_api_key(self) -> str:
        """Secret → env fallback for TMDB key."""
        try:
            api_key = config.dbutils.secrets.get("markscope", "tmdb-api-key").strip()
        except Exception:
            api_key = os.getenv("TMDB_API_KEY", "").strip()
        if not api_key:
            raise ValueError("❌ TMDB API key not found in secrets or environment variables.")
        return api_key

    # ------------------------------------------------------------
    def _fetch_page(self, api_key: str, page: int) -> dict | None:
        """Fetch one TMDB page with simple retry/backoff."""
        url = f"{TMDB_API_URL}?api_key={api_key}&language=en-US&page={page}"
        attempts = 0
        while attempts <= MAX_RETRIES:
            resp = requests.get(url, timeout=API_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            self.logger.warning(f"TMDB page {page} failed ({resp.status_code}). Retrying...")
            attempts += 1
            time.sleep(RETRY_BACKOFF)
        self.logger.error(f"❌ TMDB page {page} failed after {MAX_RETRIES} retries.")
        return None

    # ------------------------------------------------------------
    def run(self, _: dict | None = None):
        t0 = time.time()
        self.logger.info("🚀 Starting TMDB extraction job")

        api_key = self._get_tmdb_api_key()

        # 1) Download TMDB pages (JSON)
        all_pages = []
        for page in range(1, PAGE_LIMIT + 1):
            payload = self._fetch_page(api_key, page)
            if payload:
                results = payload.get("results", [])
                all_pages.append({"page": payload.get("page"), "results": results})
                self.logger.info(f"📥 Page {page} retrieved: {len(results)} results")
            time.sleep(REQUEST_DELAY)

        if not all_pages:
            raise RuntimeError("❌ No TMDB data retrieved — check API key/network.")

        # 2) Convert JSON pages into Spark DataFrame
        json_rows = [(json.dumps(p),) for p in all_pages]
        json_schema = T.StructType([T.StructField("json_data", T.StringType())])
        df_raw = (
            self.spark.createDataFrame(json_rows, json_schema)
            .withColumn("ingest_ts", F.current_timestamp())
        )

        # 3) Parse and explode
        schema_results = T.StructType([
            T.StructField("page", T.IntegerType()),
            T.StructField("results", T.ArrayType(T.StructType([
                T.StructField("title", T.StringType()),
                T.StructField("release_date", T.StringType()),
                T.StructField("genre_ids", T.ArrayType(T.IntegerType()))
            ])))
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
                F.col("ingest_ts")
            )
            .withColumn("tmdb_year", F.substring(F.col("tmdb_release_date"), 1, 4).cast("int"))
            .dropna(subset=["tmdb_title"])
        )

        # 4) Persist to ADLS (auto-creates directories)
        (
            df_tmdb
            .repartition(1)
            .write.mode("overwrite")
            .parquet(OUTPUT_PATH)
        )
        count = df_tmdb.count()
        self.logger.info(f"💾 Wrote {count} TMDB records → {OUTPUT_PATH}")

        # 5) Metrics
        metrics = {
            "titles_total": count,
            "records_written": count,
            "duration_sec": round(time.time() - t0, 2),
            "output_path": OUTPUT_PATH,
            "page_limit": PAGE_LIMIT,
        }
        self.write_metrics(metrics, name="extract_spark_tmdb_metrics", metrics_dir=self.metrics_dir)
        self.logger.info(f"✅ Completed TMDB extraction in {metrics['duration_sec']} s")

        return df_tmdb


# ================================================================
#  Entrypoint for Databricks / Local
# ================================================================
if __name__ == "__main__":
    Step01ExtractSparkTMDB().run(None)

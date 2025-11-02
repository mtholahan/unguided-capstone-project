"""
prepare_spark_tmdb_input.py
-----------------------------------------------------------
Step 03 (Spark Refactor): Harmonize TMDB → Discogs input
Unguided Capstone – Step 8 : Deploy for Testing

Reads TMDB and Discogs JSON data from Azure Data Lake Storage (ABFSS),
applies normalization and early filtering for film soundtracks, then builds
candidate pairs for downstream matching (Parquet output only).

Author : M. Holahan
Mentor : Akhil
Branch : step8-dev
"""

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
import time

# --- Local project imports ---
from scripts.utils import normalize_for_matching_extended
from scripts.base_step import BaseStep

# ================================================================
#  Constants
# ================================================================
STORAGE_ACCOUNT = "ungcapstor01"
BASE_URI = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"

TMDB_PATH = f"{BASE_URI}/raw/tmdb/"
DISCOGS_PATH = f"{BASE_URI}/raw/discogs/"
OUTPUT_PATH = f"{BASE_URI}/intermediate/tmdb_discogs_candidates/"

FILM_OST_KEYWORDS = ["soundtrack", "score", "stage & screen", "original motion picture", "ost"]
EXCLUDE_TERMS = ["tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"]


# ================================================================
#  Optimized Step Class Definition
# ================================================================
class Step03PrepareSparkTMDBInput(BaseStep):
    """Step 03 – Prepare TMDB→Discogs Candidate Pairs using Spark. Optimized."""

    def __init__(self, spark: SparkSession):
        super().__init__("step_03_prepare_spark_tmdb_input")
        self.spark = spark

        # tighten Spark behavior
        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 64 * 1024 * 1024)  # 64MB

        self.logger.info("✅ Initialized Step 03 (Spark Optimized) [branch=step8-refactor-fast]")

    # ------------------------------------------------------------
    def _safe_count(self, df: DataFrame) -> int:
        return df.agg(F.count("*").alias("n")).collect()[0]["n"]

    # ------------------------------------------------------------
    def run(self, config: dict | None = None) -> DataFrame:
        t0 = time.time()
        self.logger.info("🎬 Step 03 – Prepare TMDB→Discogs Input (Spark, optimized)")

        # ----------------------------------------------------------
        # 1. Load and normalize inputs (schema-stable subset only)
        # ----------------------------------------------------------
        tmdb_required_cols = ["title", "release_date", "genre_ids"]
        discogs_required_cols = ["title", "year", "genre", "style"]

        tmdb_raw = self.spark.read.parquet(TMDB_PATH)
        tmdb_df = self.normalize_schema(tmdb_raw, tmdb_required_cols)

        discogs_raw = self.spark.read.parquet(DISCOGS_PATH)
        discogs_df = self.normalize_schema(discogs_raw, discogs_required_cols)

        # minimal projection early to avoid wide shuffles
        tmdb_df = tmdb_df.select(
            F.col("title").alias("tmdb_title"),
            F.col("release_date").alias("tmdb_release_date"),
            F.col("genre_ids").alias("tmdb_genre_raw"),
        )

        discogs_df = discogs_df.select(
            F.col("title").alias("discogs_title"),
            F.col("year").alias("discogs_year_raw"),
            "genre",
            "style",
        )

        # ----------------------------------------------------------
        # 2. Normalize titles, extract/clean year + genre
        # ----------------------------------------------------------
        norm_udf = F.udf(normalize_for_matching_extended, T.StringType())

        tmdb_df = (
            tmdb_df
            .withColumn("tmdb_title_norm", norm_udf(F.col("tmdb_title")))
            .withColumn(
                "tmdb_year",
                F.substring("tmdb_release_date", 1, 4).cast("int")
            )
            .withColumn(
                "tmdb_genre",
                F.col("tmdb_genre_raw").cast(T.StringType())
            )
            .select("tmdb_title", "tmdb_title_norm", "tmdb_year", "tmdb_genre")
            .filter(F.col("tmdb_year").isNotNull())
            .dropDuplicates(["tmdb_title_norm", "tmdb_year"])
        )

        # Discogs cleanup (guarantee presence of columns)
        discogs_df = (
            discogs_df
            .withColumn("discogs_title_norm", norm_udf(F.col("discogs_title")))
            .withColumn("discogs_year", F.col("discogs_year_raw").cast("int"))
            .withColumn("discogs_genre", F.concat_ws(", ", F.col("genre")))
            .withColumn("discogs_style", F.concat_ws(", ", F.col("style")))
            .withColumn(
                "blob_text",
                F.lower(
                    F.concat_ws(
                        " ",
                        F.col("discogs_title"),
                        F.col("discogs_genre"),
                        F.col("discogs_style"),
                    )
                )
            )
            .select(
                "discogs_title",
                "discogs_title_norm",
                "discogs_year",
                "discogs_genre",
                "discogs_style",
                "blob_text",
            )
            .filter(F.col("discogs_year").isNotNull())
        )

        # ----------------------------------------------------------
        # 3. Filter Discogs aggressively up front (soundtrack-ish only)
        # ----------------------------------------------------------
        include_expr = F.lit(False)
        for kw in FILM_OST_KEYWORDS:
            include_expr = include_expr | F.col("blob_text").contains(kw.lower())

        exclude_expr = F.lit(False)
        for kw in EXCLUDE_TERMS:
            exclude_expr = exclude_expr | F.col("blob_text").contains(kw.lower())

        discogs_df = (
            discogs_df
            .filter(include_expr & ~exclude_expr)
            .drop("blob_text")
            .dropDuplicates(["discogs_title_norm", "discogs_year"])
        )

        # cache post-filtered sets (stable, reused)
        tmdb_df = tmdb_df.cache()
        discogs_df = discogs_df.cache()

        tmdb_n = self._safe_count(tmdb_df)
        discogs_n = self._safe_count(discogs_df)
        self.logger.info(f"📦 TMDB rows (dedup, year!=null): {tmdb_n}")
        self.logger.info(f"🎧 Discogs soundtrack-ish rows (dedup): {discogs_n}")

        # ----------------------------------------------------------
        # 4. Generate candidates WITHOUT full crossJoin
        # ----------------------------------------------------------
        # strategy: join on |tmdb_year - discogs_year| <= 1 using broadcast(discogs_df)
        #   This avoids Cartesian explosion and endless shuffle.
        discogs_b = F.broadcast(discogs_df)

        candidates_df = (
            tmdb_df.alias("t")
            .join(
                discogs_b.alias("d"),
                (
                    F.abs(F.col("t.tmdb_year") - F.col("d.discogs_year")) <= 1
                ),
                how="inner",
            )
            .select(
                F.col("t.tmdb_title").alias("movie_ref"),
                F.col("t.tmdb_title_norm"),
                F.col("d.discogs_title_norm"),
                F.col("t.tmdb_year"),
                F.col("d.discogs_year"),
                F.col("t.tmdb_genre"),
                F.col("d.discogs_genre"),
                F.col("d.discogs_style"),
            )
        ).cache()

        candidate_n = self._safe_count(candidates_df)
        self.logger.info(f"🧩 Candidate pairs generated: {candidate_n}")

        # ----------------------------------------------------------
        # 5. Write Parquet to ADLS (no coalesce(1); scalable write)
        # ----------------------------------------------------------
        (
            candidates_df
            .repartition(F.col("tmdb_year"))
            .write
            .mode("overwrite")
            .parquet(OUTPUT_PATH)
        )

        self.logger.info(f"💾 Wrote candidates Parquet → {OUTPUT_PATH}")

        # ----------------------------------------------------------
        # 6. Metrics
        # ----------------------------------------------------------
        metrics = {
            "tmdb_records": tmdb_n,
            "discogs_records": discogs_n,
            "candidate_pairs": candidate_n,
            "duration_sec": round(time.time() - t0, 1),
            "output_path": OUTPUT_PATH,
        }
        self.write_metrics(metrics, name="step03_spark_tmdb_discogs_metrics")
        self.logger.info(f"🕒 Step 03 completed in {metrics['duration_sec']} s")

        return candidates_df

    # ------------------------------------------------------------
    @staticmethod
    def run_step(spark: SparkSession, config: dict | None = None) -> DataFrame:
        return Step03PrepareSparkTMDBInput(spark).run(config)


# ================================================================
#  Entrypoint for local / notebook testing
# ================================================================
if __name__ == "__main__":
    try:
        from pyspark.dbutils import DBUtils
        spark = (
            SparkSession.builder.appName("Step03_PrepareSparkTMDBInput_Optimized")
            .config(
                "spark.hadoop.fs.azure.account.key.ungcapstor01.dfs.core.windows.net",
                DBUtils(SparkSession.builder.getOrCreate()).secrets.get("markscope", "azure-storage-account-key"),
            )
            .getOrCreate()
        )
    except Exception:
        spark = (
            SparkSession.builder.appName("Step03_PrepareSparkTMDBInput_Local_Optimized")
            .master("local[2]")
            .getOrCreate()
        )

    Step03PrepareSparkTMDBInput.run_step(spark)
    spark.stop()

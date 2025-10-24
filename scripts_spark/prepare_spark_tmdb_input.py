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

# --- Local project imports ---
from scripts.utils import normalize_for_matching_extended
from scripts.base_step import BaseStep     # <-- your existing BaseStep class
import time


# ================================================================
#  Constants
# ================================================================
TMDB_PATH = "abfss://raw@markcapstoneadls.dfs.core.windows.net/raw/tmdb/"
DISCOGS_PATH = "abfss://raw@markcapstoneadls.dfs.core.windows.net/raw/discogs/"
OUTPUT_PATH = (
    "abfss://raw@markcapstoneadls.dfs.core.windows.net/intermediate/tmdb_discogs_candidates/"
)

FILM_OST_KEYWORDS = [
    "soundtrack", "score", "stage & screen", "original motion picture", "ost"
]
EXCLUDE_TERMS = [
    "tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"
]


# ================================================================
#  Step Class Definition
# ================================================================
class Step03PrepareSparkTMDBInput(BaseStep):
    """Spark implementation of Step 03 – Prepare TMDB→Discogs Input."""

    def __init__(self, spark: SparkSession):
        super().__init__("step_03_prepare_spark_tmdb_input")
        self.spark = spark
        self.logger.info("✅ Initialized Step 03 (Spark) [branch=step8-dev]")

    # ------------------------------------------------------------
    def run(self, config: dict | None = None) -> DataFrame:
        """Execute the Spark workflow."""
        t0 = time.time()
        self.logger.info("🎬 Starting Step 03 – Prepare TMDB→Discogs Input (Spark)")

        # --------------------------------------------------------
        # 1. Load JSON from ADLS
        # --------------------------------------------------------
        tmdb_df = (
            self.spark.read.option("multiline", True)
            .json(TMDB_PATH)
            .withColumnRenamed("title", "tmdb_title")
            .withColumnRenamed("release_date", "tmdb_release_date")
        )
        discogs_df = (
            self.spark.read.option("multiline", True)
            .json(DISCOGS_PATH)
            .withColumnRenamed("title", "discogs_title")
        )

        self.logger.info(
            f"📥 Loaded TMDB ({tmdb_df.count():,}) + Discogs ({discogs_df.count():,}) records"
        )

        # --------------------------------------------------------
        # 2. Normalize titles (via helper UDF)
        # --------------------------------------------------------
        norm_udf = F.udf(normalize_for_matching_extended, T.StringType())
        tmdb_df = tmdb_df.withColumn("tmdb_title_norm", norm_udf(F.col("tmdb_title")))
        discogs_df = discogs_df.withColumn("discogs_title_norm", norm_udf(F.col("discogs_title")))

        # --------------------------------------------------------
        # 3. Extract years & genres
        # --------------------------------------------------------
        tmdb_df = (
            tmdb_df.withColumn("tmdb_year", F.substring(F.col("tmdb_release_date"), 1, 4))
            .withColumn("tmdb_genre", F.col("genre_ids").cast(T.StringType()))
            .select("tmdb_title", "tmdb_title_norm", "tmdb_year", "tmdb_genre")
        )

        discogs_df = (
            discogs_df.withColumn("discogs_year", F.col("year").cast(T.StringType()))
            .withColumn("discogs_genre", F.concat_ws(", ", F.col("genre")))
            .withColumn("discogs_style", F.concat_ws(", ", F.col("style")))
        )

        # --------------------------------------------------------
        # 4. Filter Discogs to likely soundtracks
        # --------------------------------------------------------
        blob = F.lower(
            F.concat_ws(" ", F.col("discogs_title"), F.col("discogs_genre"), F.col("discogs_style"))
        )
        cond_include = F.lit(False)
        for kw in FILM_OST_KEYWORDS:
            cond_include = cond_include | blob.contains(kw.lower())
        cond_exclude = F.lit(False)
        for kw in EXCLUDE_TERMS:
            cond_exclude = cond_exclude | blob.contains(kw.lower())

        discogs_df = discogs_df.filter(cond_include & ~cond_exclude)
        self.logger.info(f"🎧 Filtered Discogs soundtracks: {discogs_df.count():,}")

        # --------------------------------------------------------
        # 5. Cross-join with ±1-year window constraint
        # --------------------------------------------------------
        candidates_df = (
            tmdb_df.crossJoin(discogs_df)
            .filter(F.abs(F.col("tmdb_year").cast("int") - F.col("discogs_year").cast("int")) <= 1)
            .select(
                F.col("tmdb_title").alias("movie_ref"),
                "tmdb_title_norm",
                "discogs_title_norm",
                "tmdb_year",
                "discogs_year",
                "tmdb_genre",
                "discogs_genre",
                "discogs_style",
            )
        )

        count_out = candidates_df.count()
        self.logger.info(f"🧩 Candidate pairs generated: {count_out:,}")

        # --------------------------------------------------------
        # 6. Write Parquet to ADLS
        # --------------------------------------------------------
        (
            candidates_df.coalesce(1)
            .write.mode("overwrite")
            .parquet(OUTPUT_PATH)
        )
        self.logger.info(f"💾 Wrote Parquet → {OUTPUT_PATH}")

        # --------------------------------------------------------
        # 7. Write metrics for Step 04 consumption
        # --------------------------------------------------------
        metrics = {
            "tmdb_records": tmdb_df.count(),
            "discogs_records": discogs_df.count(),
            "candidate_pairs": count_out,
            "duration_sec": round(time.time() - t0, 1),
            "output_path": OUTPUT_PATH,
        }
        self.write_metrics(metrics, name="step03_spark_tmdb_discogs_metrics")
        self.logger.info(f"🕒 Step 03 completed in {metrics['duration_sec']} s")

        return candidates_df

    # ------------------------------------------------------------
    @staticmethod
    def run_step(spark: SparkSession, config: dict | None = None) -> DataFrame:
        """Static entry point for Databricks or pytest."""
        step = Step03PrepareSparkTMDBInput(spark)
        return step.run(config)


# ================================================================
#  Entrypoint for local test execution
# ================================================================
if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Step03_PrepareSparkTMDBInput")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .master("local[2]")
        .getOrCreate()
    )
    Step03PrepareSparkTMDBInput.run_step(spark)
    spark.stop()

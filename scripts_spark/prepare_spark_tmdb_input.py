# ================================================================
#  prepare_spark_tmdb_input.py — Auto-Detect Schema Version (JSON or Flattened)
# ================================================================

from pyspark.sql import SparkSession, functions as F, types as T
import time
from scripts.utils import normalize_for_matching_extended
from scripts.base_step import BaseStep
try:
    from databricks.sdk.runtime import dbutils  # Databricks context import
except ImportError:
    import builtins
    dbutils = getattr(builtins, "dbutils", None)  # fallback for local testing


STORAGE_ACCOUNT = "ungcapstor01"

# All data is nested under the 'raw' container, not separate ones
TMDB_PATH   = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/tmdb/"
DISCOGS_PATH = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/discogs/"
OUTPUT_PATH  = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/raw/intermediate/tmdb_discogs_candidates/"

FILM_OST_KEYWORDS = ["soundtrack", "score", "stage & screen", "original motion picture", "ost"]
EXCLUDE_TERMS = ["tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"]


class Step03PrepareSparkTMDBInput(BaseStep):
    """Step 03 – Prepare TMDB→Discogs Candidate Pairs (Auto-Detect Schema)."""

    def __init__(self, spark: SparkSession):
        super().__init__("step_03_prepare_spark_tmdb_input")
        self.spark = spark
        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.shuffle.partitions", "8")
        self.logger.info("✅ Step03 initialized (auto-schema build)")

    def run(self, config: dict | None = None):
        t0 = time.time()
        self.logger.info("🚀 Running Step03 (optimized Spark job control)")

        # 1️⃣ Load TMDB
        tmdb_raw = self.spark.read.parquet(TMDB_PATH)
        tmdb_df = (
            tmdb_raw.select(
                F.col("tmdb_title"),
                F.col("tmdb_release_date"),
                F.col("tmdb_genre_ids"),
                F.substring(F.col("tmdb_release_date"), 1, 4).cast("int").alias("tmdb_year")
            )
            .dropna(subset=["tmdb_title"])
            .cache()
        )

        # 2️⃣ Load Discogs
        discogs_raw = self.spark.read.parquet(DISCOGS_PATH)
        discogs_df = (
            discogs_raw.select(
                F.col("discogs_title"),
                F.col("discogs_year").cast("int"),
                F.col("discogs_genre"),
                F.col("discogs_style"),
            )
            .dropna(subset=["discogs_title"])
            .cache()
        )

        # 3️⃣ Filter Discogs
        blob = F.lower(
            F.concat_ws(" ", F.col("discogs_title"),
                        F.array_join(F.col("discogs_genre"), " "),
                        F.array_join(F.col("discogs_style"), " "))
        )
        include = F.lit(False)
        for kw in FILM_OST_KEYWORDS:
            include = include | blob.contains(kw)
        exclude = F.lit(False)
        for kw in EXCLUDE_TERMS:
            exclude = exclude | blob.contains(kw)

        discogs_df = discogs_df.filter(include & ~exclude).cache()

        # 4️⃣ Normalize titles (single UDF pass)
        norm_udf = F.udf(normalize_for_matching_extended, T.StringType())
        tmdb_df = tmdb_df.withColumn("tmdb_title_norm", norm_udf("tmdb_title"))
        discogs_df = discogs_df.withColumn("discogs_title_norm", norm_udf("discogs_title"))

        # Materialize both DataFrames once
        tmdb_count = tmdb_df.count()
        discogs_count = discogs_df.count()
        self.logger.info(f"🎬 TMDB={tmdb_count:,}, 🎧 Discogs(filtered)={discogs_count:,}")

        # 5️⃣ Join (broadcast join on small Discogs)
        candidates_df = (
            tmdb_df.alias("t")
            .join(
                F.broadcast(discogs_df.alias("d")),
                (F.abs(F.col("t.tmdb_year") - F.col("d.discogs_year")) <= 1),
                "inner"
            )
            .select(
                F.col("t.tmdb_title").alias("movie_ref"),
                "t.tmdb_title_norm",
                "d.discogs_title_norm",
                "t.tmdb_year",
                "d.discogs_year",
                "t.tmdb_genre_ids",
                "d.discogs_genre",
                "d.discogs_style",
            )
            .cache()
        )

        # Trigger materialization once
        candidate_count = candidates_df.count()
        self.logger.info(f"🧩 Candidate pairs generated: {candidate_count:,}")

        # 6️⃣ Write once
        candidates_df.repartition(1).write.mode("overwrite").parquet(OUTPUT_PATH)
        self.logger.info(f"💾 Wrote Parquet → {OUTPUT_PATH}")

        # 7️⃣ Record metrics without recomputation
        metrics = {
            "tmdb_records": tmdb_count,
            "discogs_records": discogs_count,
            "candidate_pairs": candidate_count,
            "duration_sec": round(time.time() - t0, 1),
            "output_path": OUTPUT_PATH,
        }
        self.write_metrics(metrics, name="step03_spark_tmdb_discogs_metrics")

        self.logger.info(f"✅ Step03 finished in {metrics['duration_sec']} s")
        return candidates_df

    @staticmethod
    def run_step(spark: SparkSession, config: dict | None = None):
        return Step03PrepareSparkTMDBInput(spark).run(config)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Step03_PrepareSparkTMDBInput_AutoSchema").getOrCreate()
    Step03PrepareSparkTMDBInput.run_step(spark)
    spark.stop()

# ================================================================
#  Pure Pandas Fallback — no Spark usage at all
# ================================================================
def run_pandas_fallback(limit: int | None = None):
    """Completely Spark-free fallback using local /dbfs copies."""
    import pandas as pd
    import time
    import os
    from scripts.utils import normalize_for_matching_extended

    t0 = time.time()
    print("🐼 Running PURE Pandas fallback (no Spark jobs)")

    # Copy files down from ADLS once via dbutils.fs.cp (lightweight)
    TMDB_SRC = "abfss://raw@ungcapstor01.dfs.core.windows.net/raw/tmdb/"
    DISCOGS_SRC = "abfss://raw@ungcapstor01.dfs.core.windows.net/raw/discogs/"
    TMP_TMDB = "/dbfs/tmp/tmdb/"
    TMP_DISCOGS = "/dbfs/tmp/discogs/"

    # Ensure local temp dirs exist
    os.makedirs(TMP_TMDB, exist_ok=True)
    os.makedirs(TMP_DISCOGS, exist_ok=True)

    print("📥 Copying small sample locally from ADLS...")
    dbutils.fs.cp(TMDB_SRC, TMP_TMDB, recurse=True)
    dbutils.fs.cp(DISCOGS_SRC, TMP_DISCOGS, recurse=True)

    # Read locally as Parquet
    df_tmdb = pd.read_parquet(TMP_TMDB)
    df_discogs = pd.read_parquet(TMP_DISCOGS)
    if limit:
        df_tmdb = df_tmdb.head(limit)

    print(f"📦 Loaded TMDB={len(df_tmdb):,}, DISCOGS={len(df_discogs):,}")

    # Apply your transformation logic
    FILM_OST_KEYWORDS = ["soundtrack", "score", "stage & screen", "original motion picture", "ost"]
    EXCLUDE_TERMS = ["tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"]

    df_tmdb["tmdb_title_norm"] = df_tmdb["tmdb_title"].apply(normalize_for_matching_extended)
    df_discogs["discogs_title_norm"] = df_discogs["discogs_title"].apply(normalize_for_matching_extended)

    blob = (
        df_discogs["discogs_title"].fillna("") + " " +
        df_discogs["discogs_genre"].astype(str) + " " +
        df_discogs["discogs_style"].astype(str)
    ).str.lower()

    mask = blob.apply(lambda x: any(k in x for k in FILM_OST_KEYWORDS)) & \
           ~blob.apply(lambda x: any(k in x for k in EXCLUDE_TERMS))
    df_discogs = df_discogs[mask].copy()

    df = df_tmdb.merge(
        df_discogs,
        how="inner",
        left_on="tmdb_title_norm",
        right_on="discogs_title_norm"
    )[[
        "tmdb_title", "discogs_title", "tmdb_year", "discogs_year",
        "tmdb_genre_ids", "discogs_genre", "discogs_style"
    ]].drop_duplicates()

    # Write back locally first, then copy to ADLS
    LOCAL_OUT = "/dbfs/tmp/tmdb_discogs_candidates.parquet"
    df.to_parquet(LOCAL_OUT, index=False)
    dbutils.fs.cp("file:" + LOCAL_OUT, "abfss://raw@ungcapstor01.dfs.core.windows.net/raw/intermediate/tmdb_discogs_candidates/", recurse=True)

    print(f"💾 Output written to intermediate/tmdb_discogs_candidates/ ({len(df):,} records)")
    print(f"✅ Finished pure Pandas fallback in {round(time.time() - t0, 1)} s")
    return df

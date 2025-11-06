# ================================================================
#  prepare_tmdb_discogs_candidates.py — v3.3 (mount-less, clean)
#  ---------------------------------------------------------------
#  Purpose : Generate TMDB→Discogs candidate pairs (Pandas-only)
#  Runtime : Databricks 16.4 LTS
# ================================================================


import scripts.bootstrap  # Ensures package discovery on Databricks/local
from scripts import config
from scripts.base_step import BaseStep
from pyspark.sql import functions as F, types as T, SparkSession
from pyspark.sql.types import StringType, IntegerType
from scripts.utils import normalize_for_matching_ost_safe
import time
from functools import reduce

spark = SparkSession.builder.appName("Step03PrepareTMDBDiscogsCandidates").getOrCreate()

FILM_OST_KEYWORDS = [
    "soundtrack", "score", "stage & screen", "original motion picture", "ost"
]
EXCLUDE_TERMS = [
    "tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"
]


class Step03PrepareTMDBDiscogsCandidates(BaseStep):
    """Step 03 – Generate TMDB→Discogs candidate pairs (Silver layer, Spark-native)."""

    def __init__(self):
        super().__init__("step_03_prepare_tmdb_discogs_candidates")
        self.logger.info("✅ Step 03 initialized (Spark + Medallion)")

    # ------------------------------------------------------------
    def _path_exists(self, path: str) -> bool:
        """Check if a given ADLS/DBFS path exists."""
        try:
            return len(dbutils.fs.ls(path)) > 0
        except Exception:
            return False

    # ------------------------------------------------------------
    def _resolve_partition(self, dataset: str) -> str:
        """Resolve the most appropriate Bronze path:
        1️⃣ Prefer current run_id (if exists)
        2️⃣ Otherwise, fall back to latest known partition.
        """
        base = config.layer_path("bronze", dataset)
        current_path = f"{base}/env={config.ENV}/run_id={config.RUN_ID}"

        if self._path_exists(current_path):
            self.logger.info(f"📂 Using current run partition for {dataset}: {current_path}")
            return current_path

        self.logger.warning(f"⚠️ Current run path missing for {dataset}, falling back to latest partition.")
        latest_path = self._get_latest_partition(base)
        self.logger.info(f"📦 Using latest available partition for {dataset}: {latest_path}")
        return latest_path

    # ------------------------------------------------------------
    def _get_latest_partition(self, base_path: str) -> str:
        """Resolve latest env/run_id partition (Spark-safe)."""
        try:
            df = spark.read.option("basePath", base_path).parquet(base_path)
            if "env" not in df.columns or "run_id" not in df.columns:
                self.logger.warning(f"No partition columns found in {base_path}, returning base path.")
                return base_path

            latest = (
                df.select("env", "run_id")
                .distinct()
                .orderBy(F.col("run_id").desc())
                .limit(1)
                .collect()
            )

            if not latest:
                self.logger.warning(f"No valid partitions found under {base_path}")
                return base_path

            env = latest[0]["env"]
            run_id = latest[0]["run_id"]
            resolved = f"{base_path}/env={env}/run_id={run_id}"
            self.logger.info(f"📦 Latest partition resolved → {resolved}")
            return resolved

        except Exception as e:
            self.logger.warning(f"⚠️ Could not resolve latest partition under {base_path}: {e}")
            return base_path

    # ------------------------------------------------------------

    def run(self, limit: int | None = config.DISCOGS_MAX_TITLES):
        """Consume Bronze TMDB + Discogs → produce Silver candidate set (final iteration)."""
        t0 = time.time()
        self.logger.info("🚀 Running Step 03 (Bronze ➜ Silver) — FINAL TUNED VERSION")

        # (1) Resolve Bronze sources
        tmdb_path = self._resolve_partition("tmdb")
        discogs_path = self._resolve_partition("discogs")

        # (2) Load input data
        df_tmdb = spark.read.parquet(tmdb_path)
        df_discogs = spark.read.parquet(discogs_path)
        if limit:
            df_tmdb = df_tmdb.limit(limit)

        tmdb_count = df_tmdb.count()
        discogs_count = df_discogs.count()
        self.logger.info(f"Loaded TMDB={tmdb_count}  Discogs={discogs_count} rows")


        # (3) Normalize + prepare  ----------------------------------------------------
        normalize_udf = F.udf(normalize_for_matching_ost_safe, StringType())

        df_tmdb = (
            df_tmdb
            .withColumn("tmdb_year", F.year(F.to_date("tmdb_release_date")))
            .withColumn("tmdb_title_norm", normalize_udf(F.col("tmdb_title")))
            .withColumn(
                "tmdb_title_norm",
                F.when(F.length(F.col("tmdb_title_norm")) < 3,
                    F.lower(F.regexp_replace(F.col("tmdb_title"), r"[^a-z0-9]", "")))
                .otherwise(F.col("tmdb_title_norm"))
            )
            .select("tmdb_title_norm", "tmdb_year", "tmdb_title")
        )

        df_discogs = (
            df_discogs
            .withColumn("discogs_year", F.col("discogs_year").cast(IntegerType()))
            .withColumn("discogs_title_norm", normalize_udf(F.col("discogs_title")))
            .withColumn(
                "discogs_title_norm",
                F.when(F.length(F.col("discogs_title_norm")) < 3,
                    F.lower(F.regexp_replace(F.col("discogs_title"), r"[^a-z0-9]", "")))
                .otherwise(F.col("discogs_title_norm"))
            )
            .withColumn("discogs_genre_str", F.concat_ws(" ", F.coalesce(F.col("discogs_genre"), F.array())))
            .withColumn("discogs_style_str", F.concat_ws(" ", F.coalesce(F.col("discogs_style"), F.array())))
        )

        # 🧩 --- Final Silver Cleanup Patch (vFinal) ---
        #  Apply after normalization, before hybrid join or OST filtering
        # ---------------------------------------------------------------
        df_tmdb = (
            df_tmdb
            .filter(F.trim(F.col("tmdb_title")) != "")
            .filter(F.length(F.col("tmdb_title_norm")) > 2)
        )
        df_tmdb = df_tmdb.withColumn(
            "tmdb_title_norm",
            F.regexp_replace(F.col("tmdb_title_norm"), r"\b(19|20)\d{2}\b", "")
        )
        df_tmdb = df_tmdb.withColumn("tmdb_year", F.col("tmdb_year").cast(IntegerType()))
        df_discogs = df_discogs.withColumn("discogs_year", F.col("discogs_year").cast(IntegerType()))
        self.logger.info(f"🧹 Post-cleanup TMDB count={df_tmdb.count()}, Discogs count={df_discogs.count()}")

        # (4) Apply OST keyword filters  ---------------------------------------------
        blob = F.concat_ws(
            " ",
            F.lower(F.coalesce(F.col("discogs_title"), F.lit(""))),
            F.lower(F.coalesce(F.col("discogs_genre_str"), F.lit(""))),
            F.lower(F.coalesce(F.col("discogs_style_str"), F.lit(""))),
        )

        keep_expr = F.lit(False)
        for k in FILM_OST_KEYWORDS:
            keep_expr = keep_expr | blob.contains(k.lower())

        drop_expr = F.lit(False)
        for k in EXCLUDE_TERMS:
            drop_expr = drop_expr | blob.contains(k.lower())

        df_discogs_filtered = df_discogs.filter(keep_expr & (~drop_expr))
        count_filtered = df_discogs_filtered.count()
        self.logger.info(f"Filtered Discogs → {count_filtered} potential OSTs")

        # Cache small frames
        df_tmdb.cache()
        df_discogs_filtered.cache()

        # (5) Hybrid Join Logic — Relaxed + Fuzzy (improved)  -------------------------
        join_condition = (
            (
                F.lower(F.col("discogs_title_norm")).contains(F.lower(F.col("tmdb_title_norm"))) |
                F.lower(F.col("tmdb_title_norm")).contains(F.lower(F.col("discogs_title_norm")))
            )
            & (F.abs(F.col("tmdb_year") - F.col("discogs_year")) <= 2)
        )

        df_relaxed = (
            df_tmdb.join(df_discogs_filtered, join_condition, "inner")
            .dropDuplicates(["tmdb_title_norm", "discogs_title_norm"])
            .withColumn("match_stage", F.lit("relaxed"))
        )
        count_relaxed = df_relaxed.count()
        self.logger.info(f"🎯 Relaxed join produced {count_relaxed} pairs")

        # --- Fuzzy join with looser threshold (≤10) and null guards
        LEV_DISTANCE_MAX = 10
        FUZZY_MAX_ROWS = 10_000

        if tmdb_count * discogs_count <= FUZZY_MAX_ROWS ** 2:
            df_fuzzy = (
                df_tmdb.join(
                    df_discogs_filtered,
                    (F.abs(F.col("tmdb_year") - F.col("discogs_year")) <= 2)
                    & F.col("tmdb_title_norm").isNotNull()
                    & F.col("discogs_title_norm").isNotNull(),
                    "inner",
                )
                .withColumn("lev_dist", F.levenshtein(F.col("tmdb_title_norm"), F.col("discogs_title_norm")))
                .filter(F.col("lev_dist") <= LEV_DISTANCE_MAX)
                .withColumn("match_stage", F.lit("fuzzy10"))
            )
            count_fuzzy = df_fuzzy.count()
            self.logger.info(f"🎯 Fuzzy join (Levenshtein ≤ {LEV_DISTANCE_MAX}) → {count_fuzzy} pairs")
        else:
            self.logger.warning("⚠️ Fuzzy join skipped due to dataset size threshold")
            df_fuzzy = spark.createDataFrame([], schema=df_relaxed.schema)
            count_fuzzy = 0

        # (6) Union, plausibility filters, and lineage  -------------------------------
        df_candidates = (
            df_relaxed.unionByName(df_fuzzy, allowMissingColumns=True)
            .dropDuplicates(["tmdb_title_norm", "discogs_title_norm"])
            .filter(F.col("discogs_genre_str").rlike("(?i)(stage|screen|soundtrack|score)"))
            .filter(F.abs(F.col("tmdb_year") - F.col("discogs_year")) <= 5)
            .withColumn("is_candidate_match", F.lit(True))
            .withColumn(
                "canonical_id",
                F.sha2(F.concat_ws("|", F.col("tmdb_title_norm"), F.col("discogs_title_norm"), F.col("tmdb_year")), 256),
            )
            .withColumn("env", F.lit(config.ENV))
            .withColumn("run_id", F.lit(config.RUN_ID))
            .withColumn("load_dt", F.current_date())
        )

        count_total = df_candidates.count()
        self.logger.info(
            f"✅ Hybrid total candidates: {count_total} (relaxed={count_relaxed}, fuzzy={count_fuzzy})"
        )

        # (7) Write Silver output  -----------------------------------------------------
        output_path = config.layer_path("silver", "candidates")
        config.write_df(df_candidates, "silver", "candidates")

        duration = round(time.time() - t0, 1)
        self.logger.info(f"✅ Wrote {df_candidates.count()} rows to {output_path} in {duration}s")

        # (8) Metrics + validation
        metrics = {
            "tmdb_records": tmdb_count,
            "discogs_records": discogs_count,
            "candidate_pairs": count_total,
            "duration_sec": duration,
            "output_path": output_path,
        }
        self.write_metrics(metrics, "step03_candidates_metrics", config.METRICS_DIR)

        total_rows = df_candidates.count()
        unique_ids = df_candidates.select("canonical_id").distinct().count()
        null_titles = df_candidates.filter(
            F.col("tmdb_title_norm").isNull() | F.col("discogs_title_norm").isNull()
        ).count()

        self.logger.info(
            f"✅ Silver validation summary → total={total_rows}, unique_ids={unique_ids}, null_titles={null_titles}"
        )
        if null_titles > 0:
            self.logger.warning(f"⚠️ {null_titles} rows still have null normalized titles")

        df_tmdb.unpersist()
        df_discogs_filtered.unpersist()

        return df_candidates


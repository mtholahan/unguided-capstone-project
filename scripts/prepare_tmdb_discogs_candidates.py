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
import os, time, json, requests, io, pyarrow.parquet as pq
import pandas as pd
import fsspec
from scripts.utils import normalize_for_matching_extended


spark = SparkSession.builder.getOrCreate()


FILM_OST_KEYWORDS = [
    "soundtrack", "score", "stage & screen",
    "original motion picture", "ost"
]
EXCLUDE_TERMS = [
    "tv", "series", "game", "anime", "broadway",
    "musical", "soap", "documentary"
]


class Step03PrepareTMDBDiscogsCandidates(BaseStep):
    """Step 03 – Generate TMDB→Discogs candidate pairs (Pandas-only)."""

    def __init__(self):
        super().__init__("step_03_prepare_tmdb_discogs_candidates")
        self.intermediate_dir = config.INTERMEDIATE_DIR
        self.metrics_dir = config.METRICS_DIR
        self.logger.info("✅ Step03 initialized (config-driven)")

    # ------------------------------------------------------------
    def read_parquet_abfss(self, path: str, fs) -> pd.DataFrame:
        """Read a Parquet file safely from ADLS using fsspec."""
        try:
            self.logger.info(f"📂 Reading Parquet directory from: {path}")
            # Let Spark handle directory-based Parquet datasets
            df = spark.read.parquet(path)
            return df.toPandas()
        except Exception as e:
            raise RuntimeError(f"❌ Failed to read Parquet from {path}: {e}")

    # ------------------------------------------------------------
    def run(self, limit: int | None = None) -> pd.DataFrame:
        """Run Step03 pipeline."""
        t0 = time.time()
        self.logger.info("🚀 Running Step03")

        # ADLS setup
        storage_opts = {"account_name": config.STORAGE_ACCOUNT, "anon": False}
        fs = fsspec.filesystem("abfss", **storage_opts)

        # 1️⃣ Load TMDB + Discogs
        tmdb_path = f"{self.intermediate_dir}/tmdb"
        discogs_path = f"{self.intermediate_dir}/discogs"

        df_tmdb = self.read_parquet_abfss(tmdb_path, fs)
        df_discogs = self.read_parquet_abfss(discogs_path, fs)

        if limit:
            df_tmdb = df_tmdb.head(limit)

        # 2️⃣ Normalize + filter
        df_tmdb["tmdb_year"] = pd.to_datetime(
            df_tmdb["tmdb_release_date"], errors="coerce"
        ).dt.year
        df_discogs["discogs_year"] = pd.to_numeric(
            df_discogs["discogs_year"], errors="coerce"
        )

        df_tmdb["tmdb_title_norm"] = df_tmdb["tmdb_title"].apply(
            normalize_for_matching_extended
        )
        df_discogs["discogs_title_norm"] = df_discogs["discogs_title"].apply(
            normalize_for_matching_extended
        )

        blob = (
            df_discogs["discogs_title"].fillna("") + " " +
            df_discogs["discogs_genre"].astype(str) + " " +
            df_discogs["discogs_style"].astype(str)
        ).str.lower()

        keep_mask = blob.apply(lambda x: any(k in x for k in FILM_OST_KEYWORDS))
        drop_mask = blob.apply(lambda x: any(k in x for k in EXCLUDE_TERMS))
        df_discogs = df_discogs[keep_mask & ~drop_mask].copy()

        # 3️⃣ Merge + filter by year proximity
        df = (
            df_tmdb.merge(
                df_discogs,
                how="inner",
                left_on="tmdb_title_norm",
                right_on="discogs_title_norm",
                suffixes=("_tmdb", "_discogs"),
            )
            .dropna(subset=["tmdb_year", "discogs_year"])
            .query("abs(tmdb_year - discogs_year) <= 1")
            .drop_duplicates(subset=["tmdb_title_norm", "discogs_title_norm"])
            .reset_index(drop=True)
        )

        # 4️⃣ Write results
        out_path = f"{self.intermediate_dir}/tmdb_discogs_candidates/tmdb_discogs_candidates.parquet"
        with fs.open(out_path, "wb") as f:
            df.to_parquet(f, index=False)
        self.logger.info(f"💾 Wrote candidates → {out_path}")

        # 5️⃣ Metrics
        end_time = time.time()
        duration = round(end_time - t0, 1)
        metrics = {
            "tmdb_records": len(df_tmdb),
            "discogs_records": len(df_discogs),
            "candidate_pairs": len(df),
            "duration_sec": duration,
            "output_path": out_path,
        }
        self.write_metrics(
            metrics, name="step03_candidates_metrics", metrics_dir=self.metrics_dir
        )
        self.logger.info(f"✅ Step03 completed in {duration} s")

        return df

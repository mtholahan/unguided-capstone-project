# ================================================================
#  prepare_tmdb_discogs_candidates.py
#  ---------------------------------------------------------------
#  Purpose: Generate TMDB→Discogs candidate pairs (Pandas-only)
#  Runtime: Databricks 16.4 LTS (driver-only, no Spark session)
#  Author : M. Holahan
# ================================================================

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import time
import os
import pandas as pd
from scripts.utils import normalize_for_matching_extended
from scripts.base_step import BaseStep

# ================================================================
#  Step Definition
# ================================================================
class Step03PrepareTMDBDiscogsCandidates(BaseStep):
    """Step 03: Generate TMDB→Discogs candidate pairs using Pandas."""

    def __init__(self):
        super().__init__("step_03_prepare_tmdb_discogs_candidates")

        # --- Initialize Spark minimally to register ADLS credentials ---
        self.spark = SparkSession.builder.appName("Step03_PandasPrep").getOrCreate()
        self.dbutils = DBUtils(self.spark)

        self.storage_account = self.dbutils.secrets.get("markscope", "azure-storage-account-name").strip()
        self.storage_key = self.dbutils.secrets.get("markscope", "azure-storage-account-key").strip()

        self.spark.conf.set(
            f"fs.azure.account.key.{self.storage_account}.dfs.core.windows.net",
            self.storage_key,
        )

        # --- Define paths ---
        self.base_uri = f"abfss://raw@{self.storage_account}.dfs.core.windows.net"
        self.tmdb_src = f"{self.base_uri}/raw/tmdb/"
        self.discogs_src = f"{self.base_uri}/raw/discogs/"
        self.output_dst = f"{self.base_uri}/raw/intermediate/tmdb_discogs_candidates/"

        # Local /dbfs/tmp staging area for Pandas read/write
        self.tmp_base = "/dbfs/tmp/step03_pandas"
        os.makedirs(self.tmp_base, exist_ok=True)

        self.logger.info("✅ Step03PrepareTMDBDiscogsCandidates initialized (Pandas mode)")

    # ================================================================
    #  Main execution logic
    # ================================================================
    def run(self, limit: int | None = None) -> pd.DataFrame:
        t0 = time.time()
        self.logger.info("🚀 Running Step03 (Pandas-only workflow)")

        # --- 1️⃣ Copy Parquet data from ADLS to local /dbfs/tmp ---
        tmdb_local = f"{self.tmp_base}/tmdb"
        discogs_local = f"{self.tmp_base}/discogs"
        os.makedirs(tmdb_local, exist_ok=True)
        os.makedirs(discogs_local, exist_ok=True)

        self.logger.info("📥 Copying TMDB + Discogs parquet from ADLS to /dbfs/tmp ...")
        self.dbutils.fs.cp(self.tmdb_src, f"file:{tmdb_local}", recurse=True)
        self.dbutils.fs.cp(self.discogs_src, f"file:{discogs_local}", recurse=True)

        # --- 2️⃣ Load into Pandas ---
        df_tmdb = pd.read_parquet(tmdb_local)
        df_discogs = pd.read_parquet(discogs_local)
        if limit:
            df_tmdb = df_tmdb.head(limit)

        self.logger.info(f"🎬 TMDB records loaded: {len(df_tmdb):,}")
        self.logger.info(f"🎧 Discogs records loaded: {len(df_discogs):,}")

        # --- 3️⃣ Normalize + filter Discogs ---
        FILM_OST_KEYWORDS = ["soundtrack", "score", "stage & screen", "original motion picture", "ost"]
        EXCLUDE_TERMS = ["tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"]

        df_tmdb["tmdb_year"] = pd.to_datetime(df_tmdb["tmdb_release_date"], errors="coerce").dt.year
        df_discogs["discogs_year"] = pd.to_numeric(df_discogs["discogs_year"], errors="coerce")

        df_tmdb["tmdb_title_norm"] = df_tmdb["tmdb_title"].apply(normalize_for_matching_extended)
        df_discogs["discogs_title_norm"] = df_discogs["discogs_title"].apply(normalize_for_matching_extended)

        blob = (
            df_discogs["discogs_title"].fillna("") + " " +
            df_discogs["discogs_genre"].astype(str) + " " +
            df_discogs["discogs_style"].astype(str)
        ).str.lower()

        keep_mask = blob.apply(lambda x: any(k in x for k in FILM_OST_KEYWORDS))
        drop_mask = blob.apply(lambda x: any(k in x for k in EXCLUDE_TERMS))
        df_discogs = df_discogs[keep_mask & ~drop_mask].copy()

        self.logger.info(f"📉 Discogs filtered rows: {len(df_discogs):,}")

        # --- 4️⃣ Merge and filter by year proximity ---
        df = (
            df_tmdb.merge(
                df_discogs,
                how="inner",
                left_on="tmdb_title_norm",
                right_on="discogs_title_norm",
                suffixes=("_tmdb", "_discogs"),
            )
            .query("abs(tmdb_year - discogs_year) <= 1")
            .drop_duplicates(subset=["tmdb_title_norm", "discogs_title_norm"])
            .reset_index(drop=True)
        )

        candidate_count = len(df)
        self.logger.info(f"🧩 Candidate pairs generated: {candidate_count:,}")

        # --- 5️⃣ Write Parquet locally then copy to ADLS ---
        tmp_out = f"{self.tmp_base}/tmdb_discogs_candidates.parquet"
        df.to_parquet(tmp_out, index=False)
        self.dbutils.fs.cp(f"file:{tmp_out}", self.output_dst, recurse=True)

        # --- 6️⃣ Metrics logging ---
        metrics = {
            "tmdb_records": len(df_tmdb),
            "discogs_records": len(df_discogs),
            "candidate_pairs": candidate_count,
            "duration_sec": round(time.time() - t0, 1),
            "output_path": self.output_dst,
        }
        self.write_metrics(metrics, name="step03_pandas_tmdb_discogs_metrics")
        self.logger.info(f"✅ Step03 completed in {metrics['duration_sec']} s")

        return df

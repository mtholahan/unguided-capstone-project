# ================================================================
#  validate_schema_alignment.py
#  ---------------------------------------------------------------
#  Purpose: Validate & align TMDB / Discogs schemas (Pandas-only)
#  Runtime: Databricks 16.4 LTS (driver-only, no Spark jobs)
#  Author : M. Holahan
# ================================================================

import os
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from scripts.base_step import BaseStep
from scripts.utils_schema import infer_schema, build_integrity_summary
import fsspec, shutil


# ================================================================
#  Step Definition
# ================================================================
class Step04ValidateSchemaAlignment(BaseStep):
    """Step 04: Harmonized Data Validation & Schema Alignment (Pandas-only)"""

    def __init__(self):
        super().__init__("step_04_validate_schema_alignment")

        # --- Minimal Spark init for ADLS access ---
        self.spark = SparkSession.builder.appName("Step04_PandasSchema").getOrCreate()
        self.dbutils = DBUtils(self.spark)

        self.storage_account = self.dbutils.secrets.get("markscope", "azure-storage-account-name").strip()
        self.storage_key = self.dbutils.secrets.get("markscope", "azure-storage-account-key").strip()

        self.spark.conf.set(
            f"fs.azure.account.key.{self.storage_account}.dfs.core.windows.net",
            self.storage_key,
        )

        # --- Define ADLS URIs ---
        self.base_uri = f"abfss://raw@{self.storage_account}.dfs.core.windows.net"
        self.tmdb_src = f"{self.base_uri}/raw/tmdb/"
        self.discogs_src = f"{self.base_uri}/raw/discogs/"
        self.candidates_file = f"{self.base_uri}/raw/intermediate/tmdb_discogs_candidates/tmdb_discogs_candidates.parquet"
        self.output_dst = f"{self.base_uri}/raw/validation/schema_alignment/"

        # --- Local temp workspace ---
        self.tmp_base = "/dbfs/tmp/step04_pandas"
        os.makedirs(self.tmp_base, exist_ok=True)

        self.logger.info("✅ Step04ValidateSchemaAlignment initialized (Pandas mode)")

    # ================================================================
    #  Main execution logic
    # ================================================================
    def run(self) -> pd.DataFrame:
        t0 = time.time()
        self.logger.info("🚀 Running Step04 (Pandas-only validation workflow)")

        # --- 1️⃣ Load directly from ADLS into Pandas using authenticated storage options ---
        self.logger.info("📖 Reading TMDB + Discogs data directly from ADLS (authenticated Pandas read)...")

        storage_opts = {
            "account_name": self.storage_account,
            "account_key": self.storage_key,
        }

        tmdb_df = pd.read_parquet(self.tmdb_src, storage_options=storage_opts)
        discogs_df = pd.read_parquet(self.discogs_src, storage_options=storage_opts)
        candidates_df = pd.read_parquet(self.candidates_file, storage_options=storage_opts)

        self.logger.info(f"🎬 TMDB rows: {len(tmdb_df):,}")
        self.logger.info(f"🎧 Discogs rows: {len(discogs_df):,}")

        # --- 2️⃣ Schema inference ---
        tmdb_schema = infer_schema(tmdb_df)
        discogs_schema = infer_schema(discogs_df)

        # --- 3️⃣ Integrity summary ---
        integrity_df = build_integrity_summary(tmdb_df, discogs_df, candidates_df, self.logger)

        # --- 4️⃣ Write outputs locally then to ADLS ---
        tmdb_schema_path = f"{self.tmp_base}/schema_tmdb.csv"
        discogs_schema_path = f"{self.tmp_base}/schema_discogs.csv"
        integrity_path = f"{self.tmp_base}/integrity_summary.csv"

        tmdb_schema.to_csv(tmdb_schema_path, index=False)
        discogs_schema.to_csv(discogs_schema_path, index=False)
        integrity_df.to_csv(integrity_path, index=False)

        # ✅ Ensure ADLS destination directory exists
        try:
            self.dbutils.fs.mkdirs(self.output_dst)
            self.logger.info(f"📁 Created ADLS directory (if missing): {self.output_dst}")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not create ADLS folder {self.output_dst}: {e}")

        # ✅ Safe ADLS upload using fsspec instead of dbutils.fs.cp
        storage_opts = {
            "account_name": self.storage_account,
            "account_key": self.storage_key,
        }

        fs = fsspec.filesystem("abfss", **storage_opts)

        for local_file in [tmdb_schema_path, discogs_schema_path, integrity_path]:
            remote_name = os.path.basename(local_file)
            remote_path = f"{self.output_dst.rstrip('/')}/{remote_name}"

            with open(local_file, "rb") as fsrc, fs.open(remote_path, "wb") as fdst:
                shutil.copyfileobj(fsrc, fdst)

            self.logger.info(f"📤 Uploaded {remote_name} → {remote_path}")


        # --- 5️⃣ Metrics ---
        metrics = {
            "tmdb_rows": len(tmdb_df),
            "discogs_rows": len(discogs_df),
            "tmdb_columns": tmdb_df.shape[1],
            "discogs_columns": discogs_df.shape[1],
            "tmdb_schema_path": self.output_dst + "schema_tmdb.csv",
            "discogs_schema_path": self.output_dst + "schema_discogs.csv",
            "integrity_summary_path": self.output_dst + "integrity_summary.csv",
            "duration_sec": round(time.time() - t0, 1),
        }

        self.write_metrics(metrics, name="step04_schema_validation_metrics")
        self.logger.info(f"✅ Step04 completed in {metrics['duration_sec']} s")

        return integrity_df


if __name__ == "__main__":
    Step04ValidateSchemaAlignment().run()
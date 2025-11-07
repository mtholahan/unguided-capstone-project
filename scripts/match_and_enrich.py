# ================================================================
#  match_and_enrich.py — v5.0 (Medallion-aware, config-driven)
#  ---------------------------------------------------------------
#  Purpose : Fuzzy-match TMDB ↔ Discogs candidate titles,
#            produce Gold enriched match table + lineage metadata.
# ================================================================

import scripts.bootstrap
from scripts import config
from scripts.base_step import BaseStep

import pandas as pd
from rapidfuzz import fuzz
import fsspec, os, time, json

# Config-driven thresholds
FUZZ_THRESHOLD = getattr(config, "FUZZ_THRESHOLD", 85)
YEAR_VARIANCE = getattr(config, "YEAR_VARIANCE", 2)

class Step05MatchAndEnrich(BaseStep):
    """Step 05 – Gold layer: match & enrich TMDB–Discogs pairs."""

    def __init__(self):
        super().__init__("step_05_match_and_enrich")
        self.fs = fsspec.filesystem("abfss",
            account_name=config.STORAGE_ACCOUNT,
            anon=False
        )
        self.logger.info("✅ Step05 initialized (Medallion-aware)")

    # ------------------------------------------------------------
    def run(self) -> pd.DataFrame:
        t0 = time.time()
        self.logger.info("🚀 Running Step05 — Fuzzy match & enrich")

        # === 1️⃣ Load Silver candidates (input) ===
        silver_path = config.layer_path("silver", "candidates")
        df = pd.read_parquet(silver_path,
            storage_options={"account_name": config.STORAGE_ACCOUNT, "anon": False})
        self.logger.info(f"📦 Loaded {len(df):,} Silver candidate rows")

        # --- Defensive column normalization ---
        required_cols = [
            "tmdb_title", "discogs_title",
            "tmdb_year", "discogs_year",
            "tmdb_id", "discogs_id"
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"❌ Missing required columns in Silver: {missing}")

        # === 2️⃣ Fuzzy matching ===
        self.logger.info("🎯 Computing fuzzy title similarity")
        df["match_score"] = df.apply(
            lambda r: fuzz.token_set_ratio(str(r["tmdb_title"]), str(r["discogs_title"])), axis=1
        )

        df["year_diff"] = abs(df["tmdb_year"] - df["discogs_year"])
        df["is_match"] = (df["match_score"] >= FUZZ_THRESHOLD) & (df["year_diff"] <= YEAR_VARIANCE)

        df_matches = df[df["is_match"]].copy()
        self.logger.info(f"✅ Found {len(df_matches):,} strong matches")

        # === 3️⃣ Enrichment logic (Gold shaping) ===
        df_matches["gold_ref"] = (
            df_matches["tmdb_id"].astype(str) + "_" + df_matches["discogs_id"].astype(str)
        )

        # minimal lineage fields
        df_matches["bronze_source_tmdb"] = config.layer_path("bronze", "tmdb")
        df_matches["bronze_source_discogs"] = config.layer_path("bronze", "discogs")
        df_matches["silver_source"] = silver_path
        df_matches["validated_on"] = time.strftime("%Y-%m-%dT%H:%M:%S")

        # === 4️⃣ Write to Gold layer ===
        gold_path = config.layer_path("gold", "matches")
        os.makedirs(os.path.dirname(gold_path), exist_ok=True)

        # Ensure explicit file path with .parquet extension
        gold_file_path = os.path.join(gold_path, f"matches_{config.RUN_ID}.parquet")

        df_matches.to_parquet(
            gold_file_path,
            engine="pyarrow",
            index=False,
            storage_options={"account_name": config.STORAGE_ACCOUNT, "anon": False}
        )

        self.logger.info(f"💾 Gold written to: {gold_file_path}")


        # === 5️⃣ Build metrics & lineage summary ===
        metrics = {
            "total_candidates": len(df),
            "strong_matches": len(df_matches),
            "fuzz_threshold": FUZZ_THRESHOLD,
            "year_variance": YEAR_VARIANCE,
            "match_rate_pct": round((len(df_matches) / len(df)) * 100, 2),
            "duration_sec": round(time.time() - t0, 1),
            "gold_output": gold_path,
            "env": config.ENV,
            "run_id": config.RUN_ID,
        }

        # Write metrics JSON
        self.write_metrics(metrics, "step05_match_metrics", self.metrics_dir)

        # Lineage JSON for audit
        lineage = {
            "inputs": {
                "silver_candidates": silver_path,
                "bronze_tmdb": config.layer_path("bronze", "tmdb"),
                "bronze_discogs": config.layer_path("bronze", "discogs"),
            },
            "output": gold_path,
            "records": {
                "candidates_total": len(df),
                "matches_total": len(df_matches)
            },
            "timestamp": metrics["duration_sec"],
            "match_rules": {
                "fuzz_threshold": FUZZ_THRESHOLD,
                "year_variance": YEAR_VARIANCE,
                "metric_fields": ["match_score", "year_diff", "is_match"]
            }
        }

        lineage_path = config.layer_path("gold", "matches_lineage.json")
        with self.fs.open(lineage_path, "w") as f:
            json.dump(lineage, f, indent=2)

        self.logger.info(f"🧾 Lineage written to {lineage_path}")
        self.logger.info(f"🏁 Step05 completed in {metrics['duration_sec']} s")

        return df_matches

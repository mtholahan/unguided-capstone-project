"""Step 03B: Rehydrate GUIDs
----------------------------
Adds MusicBrainz GUIDs (MBIDs) for release_group and artist_credit
to release_enriched.tsv → release_enriched_guided.tsv.

Handles headerless TSVs gracefully and respects ROW_LIMIT.
"""

from base_step import BaseStep
from pathlib import Path
from config import MB_CLEANSED_DIR, ROW_LIMIT
import pandas as pd
import csv


class Step03BRehydrateGuids(BaseStep):
    def __init__(self, name="Step 03B: Rehydrate GUIDs"):
        super().__init__(name=name)

    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 03B: Rehydrate GUIDs")

        # --- File paths ---
        release_path = MB_CLEANSED_DIR / "release_enriched.tsv"
        rg_path = MB_CLEANSED_DIR / "release_group.tsv"
        ac_path = MB_CLEANSED_DIR / "artist_credit.tsv"
        output_path = MB_CLEANSED_DIR / "release_enriched_guided.tsv"

        # --- Skip if already processed ---
        if output_path.exists():
            self.logger.info(f"✅ Output already exists: {output_path.name}. Skipping Step 03B.")
            return

        # --- Sanity checks ---
        for label, path in {
            "release_enriched.tsv": release_path,
            "release_group.tsv": rg_path,
            "artist_credit.tsv": ac_path,
        }.items():
            if not path.exists():
                self.fail(f"Missing input file: {path}")
                return

        csv.field_size_limit(1_000_000)

        # --- Load release_enriched.tsv ---
        self.logger.info(f"📥 Loading {release_path.name}...")
        read_kwargs = dict(sep="\t", low_memory=False)
        if ROW_LIMIT:
            read_kwargs["nrows"] = ROW_LIMIT
            self.logger.info(f"🔸 ROW_LIMIT active: loading first {ROW_LIMIT:,} rows.")

        release = pd.read_csv(release_path, **read_kwargs)

        # --- Detect and apply schema for headerless TSVs ---
        first_col = str(release.columns[0])
        if first_col.isdigit() or first_col == "0":
            self.logger.warning(f"⚠️ Detected headerless TSV in {release_path.name} — applying MusicBrainz schema.")
            release.columns = [
                "id", "gid", "name", "artist_credit", "release_group",
                "status", "packaging", "language", "script",
                "barcode", "comment", "edits_pending", "quality",
                "last_updated", "release_year"
            ][: len(release.columns)]

        self.logger.info(f"✅ Schema applied: {len(release.columns)} columns detected.")
        self.logger.debug(f"Columns: {release.columns.tolist()}")

        # --- Load reference tables ---
        self.logger.info("📥 Loading lookup tables...")
        rg = pd.read_csv(rg_path, sep="\t", header=None, low_memory=False, usecols=[0, 1])
        ac = pd.read_csv(ac_path, sep="\t", header=None, low_memory=False, usecols=[0, 6])
        self.logger.info(f"✅ release_group: {len(rg):,} rows | artist_credit: {len(ac):,} rows")

        # --- Build lookup maps ---
        rg_map = dict(zip(rg[0].astype(str), rg[1]))
        ac_map = dict(zip(ac[0].astype(str), ac[6]))
        self.logger.info(f"🔧 Built lookup maps: release_group={len(rg_map):,}, artist_credit={len(ac_map):,}")

        # --- Verify columns exist ---
        col_candidates = {c.lower(): c for c in release.columns}
        rg_col = next((col_candidates[k] for k in col_candidates if "release_group" in k), None)
        ac_col = next((col_candidates[k] for k in col_candidates if "artist_credit" in k), None)

        if not rg_col or not ac_col:
            self.fail(f"❌ Missing required columns: release_group or artist_credit in {release_path.name}")
            self.logger.info(f"Available columns: {list(release.columns)}")
            return

        self.logger.info(f"🧩 Mapping columns: release_group → {rg_col}, artist_credit → {ac_col}")

        # --- Apply mappings with progress bars ---
        self.logger.info("🔗 Applying GUID mappings...")
        for _ in self.progress_iter([1], desc="Mapping release_group_gid"):
            release["release_group_gid"] = release[rg_col].astype(str).map(rg_map)

        for _ in self.progress_iter([1], desc="Mapping artist_credit_gid"):
            release["artist_credit_gid"] = release[ac_col].astype(str).map(ac_map)

        # --- Calculate mapping coverage ---
        coverage_rg = release["release_group_gid"].notna().mean() * 100
        coverage_ac = release["artist_credit_gid"].notna().mean() * 100
        self.logger.info(
            f"📊 Mapping coverage: release_group_gid={coverage_rg:.1f}%, artist_credit_gid={coverage_ac:.1f}%"
        )

        # --- Save results ---
        release.to_csv(output_path, sep="\t", index=False)
        self.logger.info(f"💾 Wrote enriched file: {output_path}")

        # --- Sample preview ---
        sample = release.loc[
            release["release_group_gid"].notna() | release["artist_credit_gid"].notna(),
            [col for col in ["id", "name", ac_col, "artist_credit_gid", rg_col, "release_group_gid"] if col in release.columns]
        ].head(10)

        if not sample.empty:
            self.logger.info("🔎 Sample of enriched rows:")
            for _, row in sample.iterrows():
                self.logger.info(
                    f"   id={row.get('id', '?')} | name={str(row.get('name', ''))[:40]} | " # max characters to display for long names in logs
                    f"{ac_col}={row.get(ac_col)} → {row.get('artist_credit_gid')} | "
                    f"{rg_col}={row.get(rg_col)} → {row.get('release_group_gid')}"
                )

        # --- Metrics logging ---
        metrics = {
            "rows_total": len(release),
            "coverage_release_group": round(coverage_rg, 2),
            "coverage_artist_credit": round(coverage_ac, 2),
            "row_limit_active": bool(ROW_LIMIT)
        }
        self.write_metrics("step03b_rehydrate_guids", metrics)
        self.logger.info(f"📊 Metrics recorded: {metrics}")
        self.logger.info("✅ [DONE] Step 03B completed successfully.")


if __name__ == "__main__":
    Step03BRehydrateGuids().run()

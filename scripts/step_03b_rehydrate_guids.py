"""
Step 03B: Rehydrate GUIDs
-------------------------
Adds stable MusicBrainz GUIDs (MBIDs) for release_group and artist_credit
to release_enriched.tsv → release_enriched_guided.tsv.

This fixes broken numeric foreign keys across MB dumps by mapping:
    release.release_group → release_group.gid
    release.artist_credit → artist_credit.gid

Expected inputs:
    - release_enriched.tsv
    - release_group.tsv   (no header)
    - artist_credit.tsv   (no header)

Outputs:
    - release_enriched_guided.tsv  (same columns + *_gid fields)
"""

from base_step import BaseStep
from config import MB_CLEANSED_DIR, ROW_LIMIT, DEBUG_MODE
from utils import make_progress_bar  # ✅ unified progress helper
import pandas as pd
import csv


class Step03BRehydrateGuids(BaseStep):
    def __init__(self, name="Step 03B: Rehydrate GUIDs"):
        super().__init__(name=name)

    def run(self):
        # --- Paths ---
        release_path = MB_CLEANSED_DIR / "release_enriched.tsv"
        rg_path = MB_CLEANSED_DIR / "release_group.tsv"
        ac_path = MB_CLEANSED_DIR / "artist_credit.tsv"
        output_path = MB_CLEANSED_DIR / "release_enriched_guided.tsv"

        # --- Sanity checks ---
        for p in [release_path, rg_path, ac_path]:
            if not p.exists():
                self.fail(f"Missing input file: {p}")
                return

        self.logger.info("📥 Loading input files...")
        csv.field_size_limit(1_000_000)

        # --- Load release_enriched (has headers) ---
        release = pd.read_csv(release_path, sep="\t", low_memory=False)
        n_release = len(release)
        self.logger.info(f"Loaded release_enriched ({n_release:,} rows).")

        # --- Load headerless lookup tables ---
        rg = pd.read_csv(rg_path, sep="\t", header=None, low_memory=False, usecols=[0, 1])
        ac = pd.read_csv(ac_path, sep="\t", header=None, low_memory=False, usecols=[0, 6])
        self.logger.info(f"Loaded release_group ({len(rg):,}) and artist_credit ({len(ac):,}) reference tables.")

        # --- Build maps ---
        rg_map = dict(zip(rg[0].astype(str), rg[1]))
        ac_map = dict(zip(ac[0].astype(str), ac[6]))
        self.logger.info(f"Built lookup maps: release_group ({len(rg_map):,}), artist_credit ({len(ac_map):,}).")

        # --- Apply mappings with progress (no tqdm.pandas) ---
        total_rows = len(release)
        desc = "Mapping GUIDs"
        with make_progress_bar(total=total_rows, desc=desc, unit="rows", leave=False) as bar:
            release["release_group_gid"] = release["release_group"].astype(str).map(rg_map)
            bar.update(total_rows * 0.5)  # halfway mark (release_group)
            release["artist_credit_gid"] = release["artist_credit"].astype(str).map(ac_map)
            bar.update(total_rows * 0.5)  # finish

        # --- Coverage diagnostics ---
        coverage_rg = release["release_group_gid"].notna().mean() * 100
        coverage_ac = release["artist_credit_gid"].notna().mean() * 100
        self.logger.info(
            f"📊 Mapping coverage: release_group_gid={coverage_rg:.1f}%, artist_credit_gid={coverage_ac:.1f}%"
        )

        # --- Write output ---
        release.to_csv(output_path, sep="\t", index=False)
        self.logger.info(f"✅ Wrote enriched file: {output_path}")

        # --- Preview sample ---
        sample = release.loc[
            release["release_group_gid"].notna() | release["artist_credit_gid"].notna(),
            ["id", "name", "artist_credit", "artist_credit_gid", "release_group", "release_group_gid"]
        ].head(10)

        self.logger.info("🔎 Sample of enriched rows:")
        for _, row in sample.iterrows():
            self.logger.info(
                f"   id={row['id']} | name={row['name'][:40]} | "
                f"artist_credit={row['artist_credit']} → {row['artist_credit_gid']} | "
                f"release_group={row['release_group']} → {row['release_group_gid']}"
            )

        self.logger.info("✅ [DONE] Step 03B completed successfully.")


if __name__ == "__main__":
    step = Step03BRehydrateGuids()
    step.run()

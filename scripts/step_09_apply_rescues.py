"""Step 09: Apply Rescues (Enhanced & Polished)
-----------------------------------------------
Hybrid manual rescue pass to fix known false negatives or missing OSTs.
Adds metrics, progress_iter, and unified logging style.
"""

from base_step import BaseStep
import pandas as pd
from config import TMDB_DIR, DEBUG_MODE
import os


class Step09ApplyRescues(BaseStep):
    def __init__(self, name="Step 09: Apply Manual Rescues (Enhanced & Polished)", threshold: float = 90.0):
        super().__init__(name=name)
        self.threshold = threshold

        self.match_file = TMDB_DIR / "tmdb_match_results.csv"
        self.rescue_file = TMDB_DIR / "rescues.csv"
        self.output_enhanced = TMDB_DIR / "tmdb_match_results_enhanced.csv"
        self.audit_file = TMDB_DIR / "rescue_audit.csv"

    # ------------------------------------------------------------------
    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 09: Apply Manual Rescues (Polished)")

        # --- Load fuzzy match results ---
        if not self.match_file.exists() or os.path.getsize(self.match_file) == 0:
            self.logger.warning(f"🪫 No matches found in {self.match_file}; skipping Step 09.")
            return

        try:
            df = pd.read_csv(self.match_file, dtype=str)
        except pd.errors.EmptyDataError:
            self.logger.warning(f"🪫 {self.match_file} is empty; skipping Step 09.")
            return

        if df.empty:
            self.logger.warning(f"🪫 {self.match_file} has 0 rows; skipping Step 09.")
            return

        df.columns = [c.lower().strip() for c in df.columns]
        if "score" in df.columns:
            df.rename(columns={"score": "match_score"}, inplace=True)

        title_col = next((c for c in df.columns if "title" in c and "tmdb" in c), "tmdb_title")
        year_col = next((c for c in df.columns if "year" in c and "tmdb" in c), "tmdb_year")

        df["match_score"] = pd.to_numeric(df.get("match_score", 0), errors="coerce").fillna(0)
        final_df = df[df["match_score"] >= self.threshold].copy()
        before_rows = len(final_df)

        audit_records = []
        rescued_count = skipped_count = overridden_count = manual_only = 0

        # --- Load rescues ---
        if not self.rescue_file.exists():
            self.logger.warning("📭 No rescues.csv file found; skipping manual rescues.")
            return

        self.logger.info(f"🛟 Loading rescues from {self.rescue_file.name}")
        rescues = pd.read_csv(self.rescue_file, dtype=str)
        rescues.columns = [c.lower().strip() for c in rescues.columns]

        for _, row in self.progress_iter(rescues.iterrows(), desc="Applying Rescues"):
            title = str(row.get("golden_title", "")).strip()
            year = str(row.get("expected_year", "")).strip()
            artist = str(row.get("expected_artist", "")).strip()
            rgid = str(row.get("release_group_id", "")).strip()
            tmdb_id = str(row.get("tmdb_id", "")).strip()
            override = str(row.get("override", "False")).lower() in ("true", "1", "yes")

            if not title:
                self.logger.warning("⚠️ Skipping rescue with missing title field.")
                continue

            exists_mask = final_df[title_col].str.lower().eq(title.lower())
            if year_col in final_df.columns and year:
                exists_mask &= final_df[year_col].astype(str).eq(year)

            if exists_mask.any():
                if override:
                    overridden_count += 1
                    final_df = final_df[~exists_mask]
                    action = "overridden"
                    self.logger.info(f"🔄 Overriding existing match for {title} ({year})")
                else:
                    skipped_count += 1
                    action = "skipped"
                    self.logger.info(f"✅ Skipping rescue for {title} ({year}) — already matched")
                    audit_records.append({"title": title, "year": year, "artist": artist, "action": action})
                    continue
            else:
                action = "injected"
                manual_only += 1

            rescue_entry = {
                "tmdb_id": tmdb_id,
                "tmdb_title": title,
                "tmdb_year": year,
                "mb_artist": artist,
                "release_group_id": rgid,
                "match_source": "rescue",
                "match_score": 100.0,
            }

            final_df = pd.concat([final_df, pd.DataFrame([rescue_entry])], ignore_index=True)
            rescued_count += 1
            self.logger.info(f"🛟 Injected rescue match for {title} ({year}) [tmdb_id={tmdb_id}]")

            audit_records.append({"title": title, "year": year, "artist": artist, "action": action})

        # --- Summary ---
        after_rows = len(final_df)
        self.logger.info("📊 Rescue Summary")
        self.logger.info(f"   Injected:     {rescued_count}")
        self.logger.info(f"   Manual-only:  {manual_only}")
        self.logger.info(f"   Skipped:      {skipped_count}")
        self.logger.info(f"   Overridden:   {overridden_count}")
        self.logger.info(f"   Before:       {before_rows}")
        self.logger.info(f"   After:        {after_rows}")

        # --- Write audit + enhanced outputs ---
        if audit_records:
            pd.DataFrame(audit_records).to_csv(self.audit_file, index=False, encoding="utf-8", newline="")
            self.logger.info(f"🧾 Rescue audit saved → {self.audit_file.name} ({len(audit_records)} records)")

        final_df.to_csv(self.output_enhanced, index=False, encoding="utf-8", newline="")
        self.logger.info(f"💾 Enhanced matches saved → {self.output_enhanced.name} ({after_rows:,} rows)")

        # --- Metrics ---
        metrics = {
            "rows_before": before_rows,
            "rows_after": after_rows,
            "rescued_count": rescued_count,
            "manual_only": manual_only,
            "skipped": skipped_count,
            "overridden": overridden_count,
            "threshold": self.threshold,
            "row_limit_active": False,
        }
        self.write_metrics("step09_apply_rescues", metrics)
        self.logger.info(f"📈 Metrics logged: {metrics}")
        self.logger.info("✅ [DONE] Step 09 completed successfully.")


if __name__ == "__main__":
    Step09ApplyRescues().run()

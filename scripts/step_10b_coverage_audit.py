"""
Step 10B: OST Coverage Audit (Refactored & Polished)
----------------------------------------------------
Evaluates completeness of MusicBrainz soundtrack data
against TMDb or Golden Set titles, producing both per-title
audit and summary metrics.
"""

from base_step import BaseStep
import pandas as pd
from config import TMDB_DIR, DEBUG_MODE
from difflib import SequenceMatcher
import os


class Step10BCoverageAudit(BaseStep):
    def __init__(self, name="Step 10B: OST Coverage Audit", similarity_threshold=0.85):
        super().__init__(name=name)
        self.similarity_threshold = similarity_threshold

        # Inputs / Outputs
        self.tmdb_file = TMDB_DIR / "enriched_top_1000.csv"              # from Step 06
        self.mb_candidates = TMDB_DIR / "tmdb_input_candidates_clean.csv"  # from Step 07
        self.output_csv = TMDB_DIR / "coverage_audit.csv"
        self.summary_file = TMDB_DIR / "coverage_summary.txt"

    # -------------------------------------------------------------
    @staticmethod
    def normalize_title(t: str) -> str:
        """Normalize a title for fuzzy comparison."""
        return (
            str(t)
            .lower()
            .replace(":", "")
            .replace("-", "")
            .replace("&", "and")
            .replace("the ", "")
            .replace("  ", " ")
            .strip()
        )

    @staticmethod
    def similarity(a: str, b: str) -> float:
        """Compute similarity ratio between two normalized titles."""
        return SequenceMatcher(None, a, b).ratio()

    # -------------------------------------------------------------
    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 10B: OST Coverage Audit (Refactored & Polished)")

        if not self.tmdb_file.exists() or not self.mb_candidates.exists():
            self.logger.warning("🚫 Missing input files — ensure Steps 06 and 07 completed successfully.")
            return

        tmdb = pd.read_csv(self.tmdb_file, dtype=str)
        mb = pd.read_csv(self.mb_candidates, dtype=str)
        self.logger.info(f"📥 Loaded TMDb ({len(tmdb):,}) and MB ({len(mb):,}) titles")

        tmdb.columns = [c.lower().strip() for c in tmdb.columns]
        mb.columns = [c.lower().strip() for c in mb.columns]

        tmdb_title_col = next((c for c in tmdb.columns if "title" in c), None)
        mb_title_col = next((c for c in mb.columns if "title" in c), None)

        if not tmdb_title_col or not mb_title_col:
            self.fail("❌ Could not identify title columns in TMDb or MB datasets.")
            return

        tmdb["norm"] = tmdb[tmdb_title_col].apply(self.normalize_title)
        mb["norm"] = mb[mb_title_col].apply(self.normalize_title)

        audit_rows = []
        found = missing = ambiguous = 0

        # --- Compare with progress indicator ---
        for _, row in self.progress_iter(tmdb.iterrows(), desc="Auditing Coverage"):
            title = row[tmdb_title_col]
            norm = row["norm"]

            # Pre-compute similarities once
            sims = [(m, self.similarity(norm, m)) for m in mb["norm"].tolist()]
            mb_matches = [(m, s) for m, s in sims if s >= self.similarity_threshold]

            if len(mb_matches) == 0:
                status = "missing"
                missing += 1
            elif len(mb_matches) == 1:
                status = "found"
                found += 1
            else:
                status = "ambiguous"
                ambiguous += 1

            audit_rows.append({
                "tmdb_title": title,
                "normalized": norm,
                "status": status,
                "match_count": len(mb_matches),
            })

        # --- Write per-title audit ---
        audit_df = pd.DataFrame(audit_rows)
        audit_df.to_csv(self.output_csv, index=False, encoding="utf-8", newline="")
        total = len(audit_df)

        # --- Write summary file ---
        coverage = found / total if total else 0
        with open(self.summary_file, "w", encoding="utf-8") as f:
            f.write("🎯 OST Coverage Audit Summary\n")
            f.write(f"Total TMDb titles: {total}\n")
            f.write(f"Found:     {found}\n")
            f.write(f"Missing:   {missing}\n")
            f.write(f"Ambiguous: {ambiguous}\n")
            f.write(f"Coverage:  {coverage:.1%}\n")

        self.logger.info(f"✅ Audit complete: {found}/{total} found ({coverage:.1%} coverage)")
        self.logger.info(f"💾 Saved audit → {self.output_csv.name}, summary → {self.summary_file.name}")

        # --- Metrics JSON for Power BI or dashboards ---
        metrics = {
            "rows_tmdb": total,
            "found": found,
            "missing": missing,
            "ambiguous": ambiguous,
            "coverage_pct": round(coverage * 100, 2),
            "threshold": self.similarity_threshold,
        }
        self.write_metrics("step10b_coverage_audit", metrics)
        self.logger.info(f"📊 Metrics recorded: {metrics}")
        self.logger.info("✅ [DONE] Step 10B completed successfully.")


if __name__ == "__main__":
    Step10BCoverageAudit().run()

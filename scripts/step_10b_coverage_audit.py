"""
Step 10B: OST Coverage Audit
Evaluates completeness of MusicBrainz soundtrack data against TMDb or Golden Set titles.
"""

from base_step import BaseStep
import pandas as pd
from config import TMDB_DIR
import os
from difflib import SequenceMatcher


class Step10BCoverageAudit(BaseStep):
    def __init__(self, name="Step 10B: OST Coverage Audit", similarity_threshold=0.85):
        super().__init__(name)
        self.similarity_threshold = similarity_threshold

        # Input / Output
        self.tmdb_file = TMDB_DIR / "enriched_top_1000.csv"       # from Step 06
        self.mb_candidates = TMDB_DIR / "tmdb_input_candidates_clean.csv"  # from Step 07
        self.output_csv = TMDB_DIR / "coverage_audit.csv"
        self.summary_file = TMDB_DIR / "coverage_summary.txt"

    @staticmethod
    def normalize_title(t):
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
    def similarity(a, b):
        return SequenceMatcher(None, a, b).ratio()

    def run(self):
        self.logger.info("🔍 Starting OST Coverage Audit…")

        if not self.tmdb_file.exists() or not self.mb_candidates.exists():
            self.logger.warning("🚫 Missing input files. Ensure Steps 06 and 07 have completed.")
            return

        tmdb = pd.read_csv(self.tmdb_file, dtype=str)
        mb = pd.read_csv(self.mb_candidates, dtype=str)

        tmdb.columns = [c.lower().strip() for c in tmdb.columns]
        mb.columns = [c.lower().strip() for c in mb.columns]

        tmdb_title_col = next((c for c in tmdb.columns if "title" in c), None)
        mb_title_col = next((c for c in mb.columns if "title" in c), None)

        if not tmdb_title_col or not mb_title_col:
            raise ValueError("❌ Could not find title columns in TMDb or MB datasets.")

        tmdb["norm"] = tmdb[tmdb_title_col].apply(self.normalize_title)
        mb["norm"] = mb[mb_title_col].apply(self.normalize_title)

        audit_rows = []
        found, missing, ambiguous = 0, 0, 0

        for _, row in tmdb.iterrows():
            title = row[tmdb_title_col]
            norm = row["norm"]

            # find close matches
            mb_matches = [
                (m, self.similarity(norm, m))
                for m in mb["norm"].tolist()
                if self.similarity(norm, m) >= self.similarity_threshold
            ]

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

        audit_df = pd.DataFrame(audit_rows)
        audit_df.to_csv(self.output_csv, index=False)

        total = len(audit_df)
        with open(self.summary_file, "w", encoding="utf-8") as f:
            f.write("🎯 OST Coverage Audit Summary\n")
            f.write(f"Total TMDb titles: {total}\n")
            f.write(f"Found:     {found}\n")
            f.write(f"Missing:   {missing}\n")
            f.write(f"Ambiguous: {ambiguous}\n")
            f.write(f"Coverage:  {found/total:.1%}\n")

        self.logger.info(f"✅ Audit complete: {found}/{total} found ({found/total:.1%} coverage)")
        self.logger.info(f"💾 Saved details to {self.output_csv.name} and summary to {self.summary_file.name}")

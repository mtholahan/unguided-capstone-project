"""
Step 04 – Match Discogs ↔ TMDB Titles
---------------------------------
Purpose:
    Perform fuzzy matching between Discogs soundtrack titles and
    TMDB movie titles produced in Step 03. Generates a final
    cross-reference table of validated movie–soundtrack pairs.

Functionality:
    • Uses fuzzy string comparison (RapidFuzz WRatio) with semantic
      plausibility filtering and year-difference bounding.
    • Groups candidate rows by `movie_ref` to isolate comparisons.
    • Produces a scored match table and a histogram of score
      distributions for quality analysis.

Key Parameters (from config.py):
    • FUZZ_THRESHOLD – minimum acceptable similarity score.
    • YEAR_VARIANCE  – allowable difference between Discogs/TMDB years.
    • TOP_N          – retained top-ranked results per title (if applied).

Automation Highlights:
    • End-to-end automated run, no manual edits required.
    • Reads Step 03 Parquet input and writes Parquet + CSV outputs.
    • Generates metrics and histogram artifacts for visual QA.
    • Thread-safe execution and logging conform to BaseStep template.

Deliverables:
    • `data/intermediate/discogs_tmdb_matches.parquet`
    • CSV version for manual inspection
    • `discogs_tmdb_score_histogram.csv`
    • `discogs_tmdb_match_metrics.json` with summary statistics

Dependencies:
    • Requires Step 03 output (`discogs_tmdb_candidates_extended.parquet`).
    • Utilizes `rapidfuzz` for fuzzy scoring and pandas for I/O.

Author:
    Mark Holahan
Version:
    v5.0 – Oct 2025 (standardized documentation and metrics output)
"""


import time
from pathlib import Path
import pandas as pd
from rapidfuzz import fuzz, process
from base_step import BaseStep
from config import (
    INTERMEDIATE_DIR,
    FUZZ_THRESHOLD,
    YEAR_VARIANCE,
    TOP_N,
    get_safe_workers,
)
from utils import normalize_for_matching_extended


# ===============================================================
# 🔤 Helpers
# ===============================================================
def _wordset(text: str) -> set:
    """Return lowercase word set for plausibility filtering."""
    return set(str(text).lower().split()) if text else set()


def plausible_match(d_title: str, t_title: str) -> bool:
    """Quick semantic sanity check to prevent false positives."""
    a, b = _wordset(d_title), _wordset(t_title)
    if not a or not b:
        return False
    overlap = len(a & b)
    return overlap >= min(2, len(a), len(b))


# ===============================================================
# 🎬 Step 04 – Discogs ↔ TMDB Fuzzy Join
# ===============================================================
class Step04MatchDiscogsTMDB(BaseStep):
    """Match Discogs and TMDB titles using fuzzy logic and year bounds."""

    def __init__(self):
        super().__init__("step_04_match_discogs_tmdb")
        self.max_workers = get_safe_workers("default")
        self.input_file = Path(INTERMEDIATE_DIR) / "discogs_tmdb_candidates_extended.parquet"
        self.output_file = Path(INTERMEDIATE_DIR) / "discogs_tmdb_matches.parquet"
        self.histogram_file = Path(INTERMEDIATE_DIR) / "discogs_tmdb_score_histogram.csv"
        self.logger.info(f"Initialized Step 04 | Workers={self.max_workers}")

    # -----------------------------------------------------------
    # 🔍 Match worker
    # -----------------------------------------------------------
    def _match_group(self, group: pd.DataFrame) -> list[dict]:
        """Perform fuzzy matching within a movie_ref group."""
        results = []
        try:
            # Each group already contains both Discogs + TMDB info on one row
            for _, row in group.iterrows():
                d_title = row.get("discogs_title_norm")
                t_title = row.get("tmdb_title_norm")
                if not d_title or not t_title:
                    continue

                score = fuzz.WRatio(str(d_title), str(t_title))
                if score < FUZZ_THRESHOLD:
                    continue

                if not plausible_match(row.get("discogs_title_norm"), row.get("tmdb_title_norm")):
                    continue

                d_year, t_year = row.get("discogs_year"), row.get("tmdb_year")
                if (
                    pd.notna(d_year)
                    and pd.notna(t_year)
                    and abs(int(d_year) - int(t_year)) > YEAR_VARIANCE
                ):
                    continue

                results.append(
                    {
                        "movie_ref": row.get("movie_ref"),
                        "discogs_title_norm": d_title,
                        "tmdb_title_norm": t_title,
                        "discogs_year": d_year,
                        "tmdb_year": t_year,
                        "discogs_genre": row.get("discogs_genre"),
                        "tmdb_genre": row.get("tmdb_genre"),
                        "discogs_style": row.get("discogs_style"),
                        "score": score,
                    }
                )

        except Exception as e:
            self.logger.error(f"Match group failed → {e}")
        return results

    # -----------------------------------------------------------
    # 🚀 Run
    # -----------------------------------------------------------
    def run(self):
        self.logger.info("🎬 Starting Step 04: Discogs ↔ TMDB fuzzy matching")
        t0 = time.time()

        if not self.input_file.exists():
            self.logger.error(f"❌ Missing input {self.input_file}")
            return

        df = pd.read_parquet(self.input_file)
        if df.empty:
            self.logger.warning("⚠️ No candidate pairs found from Step 03.")
            return

        # Ensure required columns exist
        expected = {"discogs_title_norm", "tmdb_title_norm"}
        if not expected.issubset(df.columns):
            self.logger.error(f"❌ Missing columns {expected - set(df.columns)}")
            return

        self.logger.info(f"Loaded {len(df)} rows from Step 03 input")

        # Group by movie_ref
        results = []
        for movie, group in df.groupby("movie_ref"):
            group_res = self._match_group(group)
            if group_res:
                results.extend(group_res)

        if not results:
            self.logger.warning("⚠️ No fuzzy matches produced.")
            return

        out_df = pd.DataFrame(results)
        out_df.to_parquet(self.output_file, index=False)
        out_df.to_csv(self.output_file.with_suffix(".csv"), index=False)

        # Score histogram
        hist = (
            out_df["score"]
            .round(-1)
            .value_counts()
            .sort_index()
            .rename_axis("score_bin")
            .reset_index(name="count")
        )
        hist.to_csv(self.histogram_file, index=False)

        duration = round(time.time() - t0, 2)
        metrics = {
            "matches_total": len(out_df),
            "titles_matched": out_df["movie_ref"].nunique(),
            "avg_score": round(out_df["score"].mean(), 2),
            "duration_sec": duration,
            "fuzz_threshold": FUZZ_THRESHOLD,
            "year_variance": YEAR_VARIANCE,
        }
        self.save_metrics("discogs_tmdb_match_metrics.json", {"summary": metrics})
        self.write_metrics(metrics)

        self.logger.info(
            f"✅ Step 04 completed | {metrics['titles_matched']} titles, "
            f"{metrics['matches_total']} matches (avg={metrics['avg_score']}) in {duration:.2f}s"
        )


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step04MatchDiscogsTMDB().run()

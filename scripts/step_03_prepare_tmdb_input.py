# v2.2 — Film OST Heuristic Filter Added, Toggle via config

"""
Step 07 (Enhanced): Prepare TMDb Input with Film-Only + Heuristic OST Filtering
-------------------------------------------------------------------------------

Normalizes MusicBrainz + TMDb titles, removes non-film soundtracks,
accepts repaired years, and computes deterministic overlaps.
Adds optional heuristic filtering to retain only likely *film* soundtracks
based on title patterns (e.g., 'original motion picture', 'film', 'movie', 'ost', 'score').
The filter can be toggled via `FILM_OST_FILTER` in `config.py`.
"""

from base_step import BaseStep
from config import DATA_DIR, TMDB_DIR, DEBUG_MODE, FILM_OST_FILTER, FILM_OST_PATTERN
from utils import normalize_for_matching_extended as normalize
import pandas as pd
from datetime import datetime
import re

class Step07PrepareTMDbInput(BaseStep):
    def __init__(self, name="Step 07 (Enhanced): Prepare TMDb Input"):
        super().__init__(name=name)
        self.input_parquet = DATA_DIR / "soundtracks.parquet"
        self.tmdb_input_csv = TMDB_DIR / "enriched_top_1000.csv"
        self.output_csv = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        self.CURRENT_YEAR = datetime.now().year

    def extract_title(self, raw):
        try:
            parts = str(raw).split("|")
            for part in parts:
                p = part.strip()
                if len(p) > 3 and not all(c in "0123456789abcdef-" for c in p.lower()):
                    return p
        except Exception:
            pass
        return None

    def _filter_film_soundtracks(self, df: pd.DataFrame) -> pd.DataFrame:
        bad_patterns = r"(tv|series|game|anime|theme|sound\s*effect)"
        keep_mask = ~df["raw_row"].str.contains(bad_patterns, case=False, na=False)
        filtered = df[keep_mask].copy()
        dropped = len(df) - len(filtered)
        self.logger.info(f"🎬 Filtered out {dropped:,} non-film soundtracks; kept {len(filtered):,}.")
        return filtered

    @staticmethod
    def infer_year_from_text(text: str, current_year: int) -> int:
        if not text:
            return -1
        m = re.search(r"(19|20)\d{2}", text)
        if not m:
            return -1
        year = int(m.group(0))
        return year if 1900 <= year <= (current_year + 1) else -1

    def _apply_heuristic_film_ost_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        pattern = FILM_OST_PATTERN
        mask = df["title"].str.contains(pattern, case=False, na=False)
        before = len(df)
        filtered = df[mask].copy()
        self.logger.info(f"🎞️ Heuristic film-OST filter: kept {len(filtered):,} / {before:,} ({len(filtered)/max(before,1)*100:.1f}%)")
        return filtered

    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 07 (Enhanced): Prepare TMDb Input")

        if not self.input_parquet.exists():
            self.fail(f"Missing input file: {self.input_parquet}")
            return

        self.logger.info(f"📥 Loading MB soundtracks from {self.input_parquet}")
        df = pd.read_parquet(self.input_parquet)
        df = self._filter_film_soundtracks(df)

        self.logger.info("🧼 Extracting and normalizing MB titles")
        df["title"] = [self.extract_title(r) for r in df["raw_row"]]
        df["normalized_title"] = [normalize(t) for t in df["title"]]

        # Apply heuristic OST filter if enabled
        if FILM_OST_FILTER:
            df = self._apply_heuristic_film_ost_filter(df)

        repaired_again = 0
        def coerce_year(y, raw_text):
            nonlocal repaired_again
            try:
                y = int(float(y))
            except Exception:
                y = -1
            if not (1900 <= y <= self.CURRENT_YEAR + 1):
                inf = self.infer_year_from_text(raw_text, self.CURRENT_YEAR)
                if inf != -1:
                    repaired_again += 1
                    return inf
                return -1
            return y

        df["release_year"] = [coerce_year(y, r) for y, r in zip(df.get("release_year", -1), df["raw_row"])]

        reasons = {"short_title": 0, "digits_only": 0, "bad_year": 0, "bad_type": 0, "passed": 0}
        def is_valid(row):
            nt = str(row.get("normalized_title") or "").strip()
            if len(nt) < 3:
                reasons["short_title"] += 1; return False
            if nt.isdigit():
                reasons["digits_only"] += 1; return False
            yr = int(row.get("release_year", -1))
            if yr != -1 and not (1900 <= yr <= self.CURRENT_YEAR + 1):
                reasons["bad_year"] += 1; return False
            if "soundtrack" not in str(row.get("release_group_secondary_type", "")).lower():
                reasons["bad_type"] += 1; return False
            reasons["passed"] += 1; return True

        df = df[df.apply(is_valid, axis=1)].drop_duplicates(["normalized_title", "release_year"])
        out_df = df.rename(columns={"release_year": "year"})[
            ["normalized_title", "title", "release_group_id", "year", "release_group_secondary_type"]
        ]

        self.logger.info(f"🩹 Year repair second pass: {repaired_again:,} repairs; valid_year_rows={(out_df['year']!=-1).sum():,}")

        if (out_df["year"] != -1).any():
            yr_known = out_df[out_df["year"] != -1]
            self.logger.info(f"📅 MB year stats: min={int(yr_known['year'].min())}, median={yr_known['year'].median()}, max={int(yr_known['year'].max())}")

        out_df.to_csv(self.output_csv, index=False)
        self.logger.info(f"✅ Saved MB candidates → {self.output_csv.name} ({len(out_df):,} rows)")

        metrics = {
            "rows_mb_candidates": len(out_df),
            "year_repaired_second_pass": int(repaired_again),
            "year_known_rows": int((out_df['year'] != -1).sum()),
            "year_unknown_rows": int((out_df['year'] == -1).sum()),
            "heuristic_filter_applied": bool(FILM_OST_FILTER),
        }
        self.write_metrics("step07_prepare_tmdb_input", metrics)
        self.logger.info(f"📈 Metrics recorded: {metrics}")

        if DEBUG_MODE:
            self.logger.info("🔍 Debug preview (first 5 MB rows):")
            self.logger.info(out_df.head().to_string())

        self.logger.info("✅ [DONE] Step 07 (Enhanced + Heuristic Filter) completed successfully.")

        year_stats = out_df['year'].replace(-1, pd.NA).describe()
        self.logger.info(f"🎯 MB universe size: {len(out_df):,} "
                 f"(median year {year_stats['50%']:.0f})")
 

if __name__ == "__main__":
    Step07PrepareTMDbInput().run()

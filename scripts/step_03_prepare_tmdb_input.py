"""
step_03_prepare_tmdb_input.py
---------------------------------
Purpose:
    Extended harmonization of Discogs and TMDB data into a unified candidate
    dataset with normalized, comparable fields for fuzzy matching.

Version:
    v4.4 – Oct 2025
"""

import json
import re
import time
import concurrent.futures
from pathlib import Path
import pandas as pd

from base_step import BaseStep
from utils import normalize_for_matching_extended
from config import (
    DATA_DIR,
    DISCOGS_RAW_DIR,
    TMDB_RAW_DIR,
    INTERMEDIATE_DIR,
    GOLDEN_TITLES_TEST,
    DEFAULT_MAX_WORKERS,
)

# ===============================================================
# 🔤 Helpers
# ===============================================================

FILM_OST_KEYWORDS = [
    "soundtrack", "score", "stage & screen", "original motion picture", "ost", "motion picture"
]
EXCLUDE_TERMS = [
    "tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"
]
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")

def infer_year_from_text(text: str) -> int | None:
    if not text:
        return None
    match = YEAR_PATTERN.search(str(text))
    return int(match.group(0)) if match else None

def is_film_soundtrack(title: str, genres: list | None, styles: list | None) -> bool:
    blob = " ".join([title] + (genres or []) + (styles or [])).lower()
    if any(term in blob for term in EXCLUDE_TERMS):
        return False
    return any(kw in blob for kw in FILM_OST_KEYWORDS)


# ===============================================================
# 🎬 Step 03 – Extended Harmonization
# ===============================================================
class Step03PrepareTMDBInput(BaseStep):
    """Harmonize Discogs + TMDB data into unified candidate table (extended)."""

    def __init__(self):
        super().__init__("step_03_prepare_tmdb_input")
        self.discogs_raw_dir = Path(DISCOGS_RAW_DIR)
        self.tmdb_raw_dir = Path(TMDB_RAW_DIR)
        self.output_csv = Path(INTERMEDIATE_DIR) / "discogs_tmdb_candidates_extended.csv"
        self.output_parquet = self.output_csv.with_suffix(".parquet")
        self.max_workers = DEFAULT_MAX_WORKERS
        self.logger.info(
            f"Initialized Step 03 (extended) with {self.max_workers} workers"
        )

    # -----------------------------------------------------------
    # 🔄 Core worker – build candidate pairs
    # -----------------------------------------------------------
    def build_candidates_for_title(self, title: str) -> list[dict]:
        try:
            # ---------------------- Discogs ----------------------
            discogs_path = self.discogs_raw_dir / "plain" / f"{title}.json"
            if not discogs_path.exists():
                return []

            discogs_json = json.loads(discogs_path.read_text(encoding="utf-8"))
            discogs_results = discogs_json.get("results", [])
            if not discogs_results:
                return []

            discogs_records = []
            for r in discogs_results:
                if not is_film_soundtrack(r.get("title", ""), r.get("genre"), r.get("style")):
                    continue
                rec = {
                    "movie_ref": title,
                    "source": "discogs",
                    "title_raw": r.get("title"),
                    "title_norm": normalize_for_matching_extended(r.get("title")),
                    "year": r.get("year") or infer_year_from_text(r.get("title")),
                    "country": r.get("country"),
                    "genre": ", ".join(r.get("genre", [])),
                    "style": ", ".join(r.get("style", [])),
                }
                discogs_records.append(rec)

            # ---------------------- TMDB -------------------------
            tmdb_path = self.tmdb_raw_dir / f"{title}.json"
            tmdb_records = []
            if tmdb_path.exists():
                tmdb_json = json.loads(tmdb_path.read_text(encoding="utf-8"))
                for r in tmdb_json.get("results", []):
                    rec = {
                        "movie_ref": title,
                        "source": "tmdb",
                        "title_raw": r.get("title"),
                        "title_norm": normalize_for_matching_extended(r.get("title")),
                        "year": (
                            r.get("release_date")[:4]
                            if r.get("release_date")
                            else infer_year_from_text(r.get("title"))
                        ),
                        "genre": ", ".join(str(r.get("genre_ids", []))),
                        "style": "",
                    }
                    tmdb_records.append(rec)

            # ---------------------- Pair generation ----------------------
            # For fuzzy matching, output flattened rows (cartesian product)
            pairs = []
            for d in discogs_records:
                for t in tmdb_records:
                    pairs.append({
                        "movie_ref": title,
                        "discogs_title_norm": d["title_norm"],
                        "tmdb_title_norm": t["title_norm"],
                        "discogs_year": d["year"],
                        "tmdb_year": t["year"],
                        "discogs_genre": d["genre"],
                        "tmdb_genre": t["genre"],
                        "discogs_style": d["style"],
                    })

            # if no TMDB data, keep Discogs-only row
            if not tmdb_records and discogs_records:
                for d in discogs_records:
                    pairs.append({
                        "movie_ref": title,
                        "discogs_title_norm": d["title_norm"],
                        "tmdb_title_norm": None,
                        "discogs_year": d["year"],
                        "tmdb_year": None,
                        "discogs_genre": d["genre"],
                        "tmdb_genre": None,
                        "discogs_style": d["style"],
                    })

            return pairs

        except Exception as e:
            self.logger.error(f"{title}: build_candidates failed → {e}")
            return []

    # -----------------------------------------------------------
    # 🚀 Run
    # -----------------------------------------------------------
    def run(self):
        self.logger.info("🎬 Starting Step 03 (Extended Mode): Discogs→TMDB harmonization")
        t0 = time.time()
        all_pairs = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {
                executor.submit(self.build_candidates_for_title, title): title
                for title in GOLDEN_TITLES_TEST
            }
            for future in concurrent.futures.as_completed(future_map):
                title = future_map[future]
                try:
                    pairs = future.result()
                    if pairs:
                        all_pairs.extend(pairs)
                except Exception as e:
                    self.logger.error(f"{title}: thread failed → {e}")

        df = pd.DataFrame(all_pairs)
        if df.empty:
            self.logger.warning("No candidate pairs generated.")
        else:
            df.to_csv(self.output_csv, index=False)
            df.to_parquet(self.output_parquet, index=False)
            self.logger.info(f"📁 Saved harmonized candidate data → {self.output_csv}")

        duration = round(time.time() - t0, 2)
        metrics = {
            "titles_total": len(GOLDEN_TITLES_TEST),
            "pairs_total": len(df),
            "avg_pairs_per_title": (len(df) / len(GOLDEN_TITLES_TEST)) if df is not None else 0,
            "duration_sec": duration,
            "max_workers": self.max_workers,
        }

        self.save_metrics("tmdb_input_extended_metrics.json", {"summary": metrics})
        self.write_metrics(metrics)
        self.logger.info(f"✅ Step 03 (Extended) completed in {duration:.2f}s.")


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step03PrepareTMDBInput().run()

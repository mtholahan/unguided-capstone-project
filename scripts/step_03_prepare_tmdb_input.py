"""
step_03_prepare_tmdb_input.py
---------------------------------
Purpose:
    Harmonize Discogs and TMDB data into a unified candidate dataset
    with normalized comparable fields for fuzzy matching.

Version:
    v4.7 – Oct 2025
"""

from base_step import BaseStep
import json
import re
import time
import concurrent.futures
from pathlib import Path
import pandas as pd
import os

from utils import normalize_for_matching_extended
from config import (
    DATA_DIR,
    DISCOGS_RAW_DIR,
    TMDB_RAW_DIR,
    INTERMEDIATE_DIR,
    USE_GOLDEN_LIST,
    GOLDEN_TITLES_TEST,
    MAX_DISCOG_TITLES,
    DEFAULT_MAX_WORKERS,
)

# --- Ensure paths are resolved from project root ---
os.chdir(Path(__file__).resolve().parents[1])

# ===============================================================
# 🔤 Helpers
# ===============================================================

FILM_OST_KEYWORDS = [
    "soundtrack", "score", "stage & screen", "original motion picture", "ost", "motion picture"
]
EXCLUDE_TERMS = ["tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"]
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")

# --- Safe filename helper (consistent with Step 02) ---
def safe_filename(name: str) -> str:
    """Return a filesystem-safe version of a string (Windows-compatible)."""
    return re.sub(r"[^A-Za-z0-9_\-\.]+", "_", name)

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
    """Harmonize Discogs + TMDB data into unified candidate table."""

    def __init__(self):
        super().__init__("step_03_prepare_tmdb_input")
        self.discogs_raw_dir = Path(DISCOGS_RAW_DIR)
        self.tmdb_raw_dir = Path(TMDB_RAW_DIR)
        self.output_csv = Path(INTERMEDIATE_DIR) / "discogs_tmdb_candidates_extended.csv"
        self.output_parquet = self.output_csv.with_suffix(".parquet")
        self.max_workers = DEFAULT_MAX_WORKERS
        self.logger.info(f"Initialized Step 03 v4.5 with {self.max_workers} workers")

    # -----------------------------------------------------------
    # 🔄 Build candidate pairs
    # -----------------------------------------------------------
    def build_candidates_for_title(self, title: str) -> list[dict]:
        try:
            # ---------------------- Discogs ----------------------
            discogs_path = self.discogs_raw_dir / "plain" / f"{safe_filename(title)}.json"
            if not discogs_path.exists():
                return []

            discogs_json = json.loads(discogs_path.read_text(encoding="utf-8"))
            discogs_results = discogs_json.get("results", [])
            discogs_records = []
            for r in discogs_results:
                if not is_film_soundtrack(r.get("title", ""), r.get("genre"), r.get("style")):
                    continue
                discogs_records.append({
                    "movie_ref": title,
                    "source": "discogs",
                    "title_raw": r.get("title"),
                    "title_norm": normalize_for_matching_extended(r.get("title")),
                    "year": r.get("year") or infer_year_from_text(r.get("title")),
                    "genre": ", ".join(r.get("genre", [])),
                    "style": ", ".join(r.get("style", [])),
                })

            # ---------------------- TMDB --------------------------
            tmdb_path = self.tmdb_raw_dir / f"{safe_filename(title)}.json"
            tmdb_records = []
            if tmdb_path.exists():
                tmdb_json = json.loads(tmdb_path.read_text(encoding="utf-8"))
                # Handle both list and dict formats
                results = (
                    tmdb_json.get("results", [])
                    if isinstance(tmdb_json, dict)
                    else tmdb_json
                )

                for r in results:
                    tmdb_records.append({
                        "movie_ref": title,
                        "source": "tmdb",
                        "title_raw": r.get("title") or r.get("original_title"),
                        "title_norm": normalize_for_matching_extended(
                            r.get("title") or r.get("original_title")
                        ),
                        "year": (
                            r.get("release_date")[:4]
                            if r.get("release_date")
                            else infer_year_from_text(r.get("title"))
                        ),
                        "genre": ", ".join(map(str, r.get("genre_ids", []))),
                        "style": "",
                    })

            # ---------------------- Pair generation ----------------
            pairs = []
            if discogs_records and tmdb_records:
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
            elif discogs_records:  # fallback: Discogs only
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
        self.logger.info("🎬 Starting Step 03 v4.7: Discogs→TMDB harmonization")
        t0 = time.time()
        all_pairs = []

        # --- Determine which titles to process -----------------
        if USE_GOLDEN_LIST:
            titles_to_process = GOLDEN_TITLES_TEST
            mode = "GOLDEN"
        else:
            # --- Gather Discogs JSONs dynamically ---
            discogs_files = []
            for subdir in ["plain", "soundtrack"]:
                path = self.discogs_raw_dir / subdir
                if path.exists():
                    count = len(list(path.glob("*.json")))
                    self.logger.info(f"📂 Found {count} files in {path}")
                    discogs_files.extend(path.glob("*.json"))

            titles_to_process = [
                f.stem.replace("_", " ") for f in discogs_files
            ][:MAX_DISCOG_TITLES]

            mode = f"AUTO ({len(titles_to_process)} titles)"

        self.logger.info(f"🔧 Mode={mode} | Titles to process={len(titles_to_process)}")

        # --- Parallel execution -------------------------------
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_map = {
                executor.submit(self.build_candidates_for_title, title): title
                for title in titles_to_process
            }
            for future in concurrent.futures.as_completed(future_map):
                title = future_map[future]
                try:
                    pairs = future.result()
                    if pairs:
                        all_pairs.extend(pairs)
                except Exception as e:
                    self.logger.error(f"{title}: thread failed → {e}")

        # --- Aggregate results --------------------------------
        df = pd.DataFrame(all_pairs)
        self.logger.info(f"🧩 Candidate pairs collected: {len(df)} rows")

        if df.empty:
            self.logger.warning("⚠️ No candidate pairs generated — check TMDB/Discogs alignment.")
        else:
            df.to_csv(self.output_csv, index=False)
            df.to_parquet(self.output_parquet, index=False)
            self.logger.info(f"📁 Saved harmonized data → {self.output_csv}")
            self.logger.info(f"📊 Columns: {list(df.columns)}")

        # --- Metrics & summary --------------------------------
        duration = round(time.time() - t0, 2)
        metrics = {
            "mode": mode,
            "titles_total": len(titles_to_process),
            "pairs_total": len(df),
            "avg_pairs_per_title": (
                len(df) / len(titles_to_process)
            ) if titles_to_process else 0,
            "duration_sec": duration,
            "max_workers": self.max_workers,
        }
        self.save_metrics("tmdb_input_extended_metrics.json", {"summary": metrics})
        self.write_metrics(metrics)
        self.logger.info(f"✅ Step 03 completed in {duration:.2f}s | Mode={mode}")

# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step03PrepareTMDBInput().run()

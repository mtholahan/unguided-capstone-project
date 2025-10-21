"""
Step 03 – Prepare TMDB→Discogs Input
------------------------------------
Purpose:
    Harmonize TMDB and Discogs raw data into unified candidate
    datasets for fuzzy matching (Step 04). TMDB is the driver source.

Highlights:
    • TMDB → Discogs directional refactor (step6-dev)
    • Reads TMDB JSONs (Step 01) and Discogs JSONs (Step 02)
    • Applies text normalization and candidate pair generation
    • Parallelized execution with checkpoint-safe completion
    • Outputs CSV + Parquet candidate tables and metrics JSON

Outputs:
    data/intermediate/tmdb_discogs_candidates_extended.csv
    data/intermediate/tmdb_discogs_candidates_extended.parquet
    data/metrics/step03_tmdb_discogs_metrics.json
"""

import json
import re
import time
import concurrent.futures
import pandas as pd
from pathlib import Path
from scripts.base_step import BaseStep
from scripts.utils import normalize_for_matching_extended, safe_filename
from scripts.config import (
    DATA_DIR,
    TMDB_RAW_DIR,
    DISCOGS_RAW_DIR,
    INTERMEDIATE_DIR,
    USE_GOLDEN_LIST,
    GOLDEN_TITLES_TEST,
    DEFAULT_MAX_WORKERS,
)

# ===============================================================
# 🔤 Helper constants & functions
# ===============================================================
FILM_OST_KEYWORDS = [
    "soundtrack", "score", "stage & screen", "original motion picture", "ost", "motion picture"
]
EXCLUDE_TERMS = ["tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"]
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")


def infer_year_from_text(text: str | None):
    """Extract a 4-digit year from a title or text blob."""
    if not text:
        return None
    match = YEAR_PATTERN.search(str(text))
    return int(match.group(0)) if match else None


def is_film_soundtrack(title: str, genres: list | None, styles: list | None) -> bool:
    """Return True if a Discogs record likely represents a film soundtrack."""
    blob = " ".join([title] + (genres or []) + (styles or [])).lower()
    if any(term in blob for term in EXCLUDE_TERMS):
        return False
    return any(kw in blob for kw in FILM_OST_KEYWORDS)


# ===============================================================
# 🎬 Step 03 – TMDB→Discogs Harmonization
# ===============================================================
class Step03PrepareTMDBInput(BaseStep):
    """Harmonize TMDB + Discogs data into unified candidate table."""

    def __init__(self):
        super().__init__("step_03_prepare_tmdb_input")
        self.tmdb_raw_dir = Path(TMDB_RAW_DIR)
        self.discogs_raw_dir = Path(DISCOGS_RAW_DIR)
        self.output_csv = Path(INTERMEDIATE_DIR) / "tmdb_discogs_candidates_extended.csv"
        self.output_parquet = self.output_csv.with_suffix(".parquet")
        self.max_workers = DEFAULT_MAX_WORKERS
        self.logger.info(f"Initialized Step 03 (TMDB→Discogs) [branch=step6-dev]")

    # -----------------------------------------------------------
    def build_candidates_for_title(self, title: str) -> list[dict]:
        """Generate candidate TMDB–Discogs record pairs for one movie title."""
        try:
            tmdb_path = self.tmdb_raw_dir / f"{safe_filename(title)}.json"
            discogs_path = self.discogs_raw_dir / f"{safe_filename(title)}.json"
            pairs = []

            # --- TMDB Records ---
            if not tmdb_path.exists():
                return []
            tmdb_json = json.loads(tmdb_path.read_text(encoding="utf-8"))
            tmdb_results = tmdb_json.get("results", []) if isinstance(tmdb_json, dict) else tmdb_json

            tmdb_records = []
            for r in tmdb_results:
                tmdb_records.append({
                    "movie_ref": title,
                    "title_raw": r.get("title") or r.get("original_title"),
                    "title_norm": normalize_for_matching_extended(
                        r.get("title") or r.get("original_title") or title
                    ),
                    "year": (
                        r.get("release_date")[:4]
                        if r.get("release_date")
                        else infer_year_from_text(r.get("title"))
                    ),
                    "genre": ", ".join(map(str, r.get("genre_ids", []))),
                })

            # --- Discogs Records ---
            discogs_records = []
            if discogs_path.exists():
                discogs_json = json.loads(discogs_path.read_text(encoding="utf-8"))
                results = discogs_json.get("results", [])
                for d in results:
                    if not is_film_soundtrack(d.get("title", ""), d.get("genre"), d.get("style")):
                        continue
                    discogs_records.append({
                        "title_raw": d.get("title"),
                        "title_norm": normalize_for_matching_extended(d.get("title")),
                        "year": d.get("year") or infer_year_from_text(d.get("title")),
                        "genre": ", ".join(d.get("genre", [])),
                        "style": ", ".join(d.get("style", [])),
                    })

            # --- Build candidate pairs ---
            if tmdb_records and discogs_records:
                for t in tmdb_records:
                    for d in discogs_records:
                        pairs.append({
                            "movie_ref": title,
                            "tmdb_title_norm": t["title_norm"],
                            "discogs_title_norm": d["title_norm"],
                            "tmdb_year": t["year"],
                            "discogs_year": d["year"],
                            "tmdb_genre": t["genre"],
                            "discogs_genre": d["genre"],
                            "discogs_style": d["style"],
                        })
            elif tmdb_records:
                for t in tmdb_records:
                    pairs.append({
                        "movie_ref": title,
                        "tmdb_title_norm": t["title_norm"],
                        "discogs_title_norm": None,
                        "tmdb_year": t["year"],
                        "discogs_year": None,
                        "tmdb_genre": t["genre"],
                        "discogs_genre": None,
                        "discogs_style": None,
                    })
            return pairs

        except Exception as e:
            self.logger.error(f"{title}: build_candidates failed → {e}")
            return []

    # -----------------------------------------------------------
    # -----------------------------------------------------------
    def run(self):
        """
        Step 03 – Harmonize TMDB → Discogs candidate inputs.
        Reads the shared title list saved by Step 01 (GOLDEN or AUTO mode).
        """
        self.logger.info("🎬 Starting Step 03 (TMDB→Discogs Harmonization)")
        t0 = time.time()
        all_pairs = []

        # --- Load shared title list from Step 01 ---
        titles_path = Path(INTERMEDIATE_DIR) / "titles_to_process.json"
        if titles_path.exists():
            titles_to_process = json.loads(titles_path.read_text(encoding="utf-8"))
            mode = f"SHARED ({len(titles_to_process)} titles)"
            self.logger.info(f"📄 Loaded {len(titles_to_process)} titles from {titles_path.name}")
        else:
            tmdb_files = list(self.tmdb_raw_dir.glob("*.json"))
            titles_to_process = [f.stem.replace("_", " ") for f in tmdb_files]
            mode = f"AUTO ({len(titles_to_process)} titles)"
            self.logger.warning("⚠️ No shared title list found; reverting to AUTO mode")

        # --- Optional integrity check against Discogs files ---
        discogs_files = [f.stem for f in self.discogs_raw_dir.glob("*.json")]
        missing = [t for t in titles_to_process if safe_filename(t) not in discogs_files]
        if missing:
            self.logger.warning(
                f"⚠️ {len(missing)} titles missing Discogs JSONs — check Step 02 completeness"
            )

        self.logger.info(f"🔧 Mode={mode} | Titles to process={len(titles_to_process)}")

        # --- Parallelized candidate build ---
        import concurrent.futures
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

        # --- Aggregate and save ---
        import pandas as pd
        df = pd.DataFrame(all_pairs)
        self.logger.info(f"🧩 Candidate pairs collected: {len(df)} rows")

        if df.empty:
            self.logger.warning("⚠️ No candidate pairs generated — check TMDB/Discogs alignment.")
        else:
            # Normalize types for Parquet compatibility
            for col in ["tmdb_year", "discogs_year", "tmdb_genre", "discogs_genre", "discogs_style"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).fillna("")
            df = df.fillna("")

            df.to_csv(self.output_csv, index=False)
            df.to_parquet(self.output_parquet, index=False)
            self.logger.info(f"📁 Saved harmonized data → {self.output_csv}")
            self.logger.info(f"📊 Columns: {list(df.columns)}")

        # --- Metrics ---
        duration = round(time.time() - t0, 2)
        metrics = {
            "mode": mode,
            "titles_total": len(titles_to_process),
            "pairs_total": len(df),
            "avg_pairs_per_title": (len(df) / len(titles_to_process)) if titles_to_process else 0,
            "duration_sec": duration,
            "max_workers": self.max_workers,
            "direction": "TMDB→Discogs",
            "branch": "step6-dev",
        }

        self.write_metrics(metrics, name="step03_tmdb_discogs_metrics")
        self.logger.info(f"✅ Step 03 completed in {duration:.2f}s | Mode={mode}")


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step03PrepareTMDBInput().run()

"""
🎬 Step 03 – Prepare TMDB→Discogs Input
Unguided Capstone Project | Environment-aware, unified baseline
"""

import os, json, re, time, concurrent.futures
from pathlib import Path
import pandas as pd
from scripts.base_step import BaseStep
from scripts.utils import normalize_for_matching_extended, safe_filename
from scripts.config_env import load_and_validate_env
from scripts.config import TMDB_RAW_DIR, DISCOGS_RAW_DIR, DEFAULT_MAX_WORKERS


# ===============================================================
# 🔤 Helper constants & functions
# ===============================================================
FILM_OST_KEYWORDS = [
    "soundtrack", "score", "stage & screen", "original motion picture", "ost", "motion picture"
]
EXCLUDE_TERMS = ["tv", "series", "game", "anime", "broadway", "musical", "soap", "documentary"]
YEAR_PATTERN = re.compile(r"(19|20)\d{2}")


def infer_year_from_text(text: str | None):
    if not text:
        return None
    m = YEAR_PATTERN.search(str(text))
    return int(m.group(0)) if m else None


def is_film_soundtrack(title: str, genres: list | None, styles: list | None) -> bool:
    blob = " ".join([title] + (genres or []) + (styles or [])).lower()
    if any(term in blob for term in EXCLUDE_TERMS):
        return False
    return any(kw in blob for kw in FILM_OST_KEYWORDS)


# ===============================================================
# 🎯 Core Class
# ===============================================================
class Step03PrepareTMDBInput(BaseStep):
    """Harmonize TMDB + Discogs data into unified candidate table."""

    def __init__(self, spark=None):
        super().__init__(name="step_03_prepare_tmdb_input")
        self.spark = spark

        # ✅ Environment-aware directories
        self.output_dir = Path(os.getenv("PIPELINE_OUTPUT_DIR", "data/intermediate")).resolve()
        self.metrics_dir = Path(os.getenv("PIPELINE_METRICS_DIR", "data/metrics")).resolve()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

        # ✅ Raw input directories (allow env override)
        self.tmdb_raw_dir = Path(os.getenv("TMDB_RAW_DIR", TMDB_RAW_DIR)).resolve()
        self.discogs_raw_dir = Path(os.getenv("DISCOGS_RAW_DIR", DISCOGS_RAW_DIR)).resolve()

        # ✅ Output targets
        self.output_csv = self.output_dir / "tmdb_discogs_candidates_extended.csv"
        self.output_parquet = self.output_dir / "tmdb_discogs_candidates_extended.parquet"
        self.max_workers = int(os.getenv("MAX_WORKERS", DEFAULT_MAX_WORKERS))

        self.logger.info("Initialized Step 03 (TMDB → Discogs Input)")

    # -----------------------------------------------------------
    def build_candidates_for_title(self, title: str) -> list[dict]:
        """Generate candidate TMDB–Discogs pairs for one movie title."""
        try:
            tmdb_path = self.tmdb_raw_dir / f"{safe_filename(title)}.json"
            discogs_path = self.discogs_raw_dir / f"{safe_filename(title)}.json"
            pairs = []

            # --- TMDB records ---
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

            # --- Discogs records ---
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

            # --- Candidate pairs ---
            for t in tmdb_records:
                if discogs_records:
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
                else:
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
    def run(self):
        """Run TMDB → Discogs harmonization."""
        t0 = time.time()
        self.logger.info("🚀 Starting Step 03 | TMDB→Discogs Harmonization")
        self.logger.info(f"🕒 Run timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # --- Load titles ---
        titles_path = self.output_dir / "titles_to_process.json"
        if titles_path.exists():
            titles = json.loads(titles_path.read_text(encoding="utf-8"))
            mode = f"SHARED ({len(titles)} titles)"
        else:
            tmdb_files = list(self.tmdb_raw_dir.glob("*.json"))
            titles = [f.stem.replace("_", " ") for f in tmdb_files]
            mode = f"AUTO ({len(titles)} titles)"
            self.logger.warning("⚠️ No shared title list found; AUTO mode enabled")

        # --- Build candidates in parallel ---
        all_pairs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            for pairs in ex.map(self.build_candidates_for_title, titles):
                if pairs:
                    all_pairs.extend(pairs)

        df = pd.DataFrame(all_pairs)
        self.logger.info(f"🧩 Candidate pairs collected: {len(df)} rows")

        if not df.empty:
            for col in ["tmdb_year", "discogs_year", "tmdb_genre", "discogs_genre", "discogs_style"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).fillna("")
            df.to_csv(self.output_csv, index=False)
            df.to_parquet(self.output_parquet, index=False)
            self.logger.info(f"📁 Saved harmonized data → {self.output_csv}")
        else:
            self.logger.warning("⚠️ No candidate pairs generated")

        # --- Metrics ---
        duration = round(time.time() - t0, 2)
        metrics = {
            "mode": mode,
            "titles_total": len(titles),
            "pairs_total": len(df),
            "avg_pairs_per_title": (len(df) / len(titles)) if titles else 0,
            "duration_sec": duration,
            "max_workers": self.max_workers,
            "direction": "TMDB→Discogs",
        }
        self.write_metrics(metrics, name="step03_tmdb_discogs_metrics")
        self.logger.info(f"✅ Step 03 completed in {duration:.2f}s | Mode={mode}")


# ===============================================================
# Entrypoint
# ===============================================================
if __name__ == "__main__":
    Step03PrepareTMDBInput(None).run()

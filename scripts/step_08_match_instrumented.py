# v2.1 — Bounded fallback, plausibility check, better no-year handling

"""
Step 08: Match TMDb (Instrumented, Refactored, Detailed)
--------------------------------------------------------

This module performs the crucial fuzzy matching stage between the normalized
TMDb movie titles and the MusicBrainz (MB) soundtrack dataset. It aims to
link each TMDb title to a plausible MB soundtrack release group, based on
normalized text similarity and release year proximity.

### Core Responsibilities

1. **Data Loading and Preparation**

   * Loads the normalized TMDb dataset (`tmdb_movies_normalized.parquet`) and
     the MB soundtrack candidates (`tmdb_input_candidates_clean.csv`).
   * Ensures both datasets are consistently normalized via
     `utils.normalize_for_matching_extended()`.
   * Converts year columns to numeric and sorts records for efficient matching.

2. **Exact Matching Pass**

   * Attempts deterministic joins using both the normalized title and release year.
   * This quickly identifies direct one-to-one matches with 100% confidence.
   * Exact matches are recorded with a `score` of 100 and `match_mode = 'exact'`.

3. **Fuzzy Matching Pass**

   * For all TMDb rows without an exact match, a fuzzy search is performed.
   * The matching algorithm uses `RapidFuzz`’s `fuzz.WRatio` scorer, which handles
     token ordering, case, and partial overlaps.
   * Candidate selection is *bounded by release year proximity*:

     * First, within ±`YEAR_VARIANCE` years (minimum ±3).
     * If no candidates exist, expands to ±10 years (decade fallback).
   * This restriction prevents nonsensical matches (e.g., matching 2019 films to
     1970s sound effects albums).

4. **Alternative Title Rescue**

   * If a TMDb title yields no match above the fuzzy threshold (`FUZZ_THRESHOLD`),
     optional alternate titles are fetched from the TMDb API (if `USE_ALT_TITLES`
     and a valid API key are configured).
   * Each alternate title is normalized and re-compared; if it produces a higher
     score, the match is updated with `match_mode = 'alt_rescue'`.

5. **Plausibility Check (Token Overlap Guard)**

   * Even if a fuzzy score exceeds the threshold, some pairs may be lexically
     similar but semantically unrelated (e.g., “Star Wars” vs. “Star Trek Sound
     Effects”).
   * A final lexical plausibility filter ensures at least one 4+ character token
     overlaps between the original TMDb title and the MB title. If not, the
     candidate is downgraded to a miss with reason `implausible_overlap`.

6. **Metrics and Logging**

   * Records counts for:

     * Exact matches
     * Fuzzy-only matches
     * Alt-title rescues
     * Rows with no year information
     * Rows with no candidates found
   * Computes aggregate metrics such as average and median fuzzy scores,
     and overall match percentage.
   * Exports results to multiple formats:

     * CSV (`tmdb_match_results_enhanced.csv`)
     * CSV of unmatched rows (`tmdb_match_unmatched.csv`)
     * Parquet (`tmdb_match_results_enhanced.parquet`)
     * Fuzzy score histogram (`tmdb_fuzzy_score_histogram.csv`)

7. **Azure Upload (Optional)**

   * If Azure Blob Storage credentials are configured, outputs are uploaded to
     a designated blob container path under `outputs/`.
   * Upload failures do not interrupt pipeline execution; they only log warnings.

8. **Execution and Instrumentation**

   * The module logs detailed progress with timing, record counts, and reasons
     for failed matches.
   * Supports optional sampling (`--sample N`) for rapid debugging.
   * Controlled via configuration constants imported from `config.py`:

     * `FUZZ_THRESHOLD`: minimum fuzzy match score to accept a candidate.
     * `YEAR_VARIANCE`: default year window before expanding to ±10.
     * `TOP_N`: number of fuzzy candidates RapidFuzz returns per query.
     * `USE_ALT_TITLES`: whether to fetch TMDb alternate titles.
     * `FORCE_RENORM`: if True, renormalizes all input titles regardless of existing columns.

### Expected Behavior

After this step, the output dataset contains all TMDb movie IDs with their best
possible MusicBrainz soundtrack matches, each annotated with:

* Matched MB release title and release group ID
* Fuzzy score and match mode (exact, fuzzy, alt_rescue)
* TMDb and MB release years

Typical downstream step (Step 10B Coverage Audit) consumes this dataset to
compute coverage metrics, verifying how many Golden Test films successfully
linked to MB soundtracks.

### Improvements Over v1 Baseline

* Replaced unbounded fallback (full MB pool) with decade-bounded fallback.
* Added lexical plausibility guard to eliminate non-semantic high-score pairs.
* More explicit year validation and logging of missing-year behavior.
* Preserved instrumented debug logs (`DEBUG_MODE`) for per-title traceability.
* Introduced optional `sample` CLI argument for lightweight test runs.

### Key Outputs

| File                                  | Purpose                                      |
| ------------------------------------- | -------------------------------------------- |
| `tmdb_match_results_enhanced.csv`     | Accepted matches (exact + fuzzy + alt)       |
| `tmdb_match_unmatched.csv`            | All TMDb titles that did not match plausibly |
| `tmdb_match_results_enhanced.parquet` | Parquet copy of accepted matches             |
| `tmdb_fuzzy_score_histogram.csv`      | Score distribution for diagnostics           |

"""


from base_step import BaseStep
import pandas as pd, requests, numpy as np, os, argparse, time, re
from rapidfuzz import fuzz, process
from config import (
    TMDB_DIR, DEBUG_MODE, TMDB_API_KEY, AZURE_CONN_STR, BLOB_CONTAINER,
    FUZZ_THRESHOLD, YEAR_VARIANCE, USE_ALT_TITLES, FORCE_RENORM, TOP_N
)
from utils import normalize_for_matching_extended as normalize
from azure.storage.blob import BlobServiceClient
from pathlib import Path

# (SafeWriterMixin unchanged)
class SafeWriterMixin:
    def safe_overwrite(self, df: pd.DataFrame, path: Path, upload_to_blob=False):
        tmp = Path(f"{path}.tmp")
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
        self.logger.info(f"💾 Wrote {len(df):,} rows → {path.name}")
        if upload_to_blob:
            self.upload_to_blob(path)
    def upload_to_blob(self, local_path: Path):
        if not AZURE_CONN_STR:
            self.logger.info("☁️ Azure upload skipped (no connection string set).")
            return
        try:
            blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
            blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=f"outputs/{local_path.name}")
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            self.logger.info(f"☁️ Uploaded {local_path.name} → blob:{BLOB_CONTAINER}/outputs/")
        except Exception as e:
            self.logger.warning(f"⚠️ Azure upload failed: {e}")


def fetch_alt_titles(tmdb_id: str) -> list[str]:
    if not USE_ALT_TITLES or not TMDB_API_KEY:
        return []
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/alternative_titles"
    for attempt in range(3):
        try:
            r = requests.get(url, params={"api_key": TMDB_API_KEY}, timeout=10)
            r.raise_for_status()
            titles = [t.get("title", "") for t in r.json().get("titles", []) if t.get("title")]
            return [normalize(t) for t in titles]
        except Exception:
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
            else:
                return []


class Step08MatchTMDb(BaseStep, SafeWriterMixin):
    def __init__(self, name="Step 08: Match TMDb (Instrumented)", sample=None):
        super().__init__(name=name)
        self.tmdb_norm = TMDB_DIR / "tmdb_movies_normalized.parquet"
        self.mb_candidates = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        self.output_matches = TMDB_DIR / "tmdb_match_results_enhanced.csv"
        self.output_unmatched = TMDB_DIR / "tmdb_match_unmatched.csv"
        self.output_parquet = TMDB_DIR / "tmdb_match_results_enhanced.parquet"
        self.sample = sample

    def _ensure_normalized(self, df: pd.DataFrame, title_col: str) -> pd.DataFrame:
        needs = FORCE_RENORM or ("normalized_title" not in df.columns) or df["normalized_title"].isna().any()
        if needs:
            self.logger.info("🧼 (Re)normalizing titles via utils.normalize_for_matching_extended()")
            df["normalized_title"] = df[title_col].fillna("").map(normalize)
        return df

    @staticmethod
    def _wordset(s: str) -> set:
        return {w for w in re.findall(r"[a-z0-9]+", str(s).lower()) if len(w) >= 4}

    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 08: Match TMDb (Instrumented, Refactored)")

        # --- Load datasets ---
        self.logger.info("📥 Loading normalized TMDB + MB datasets...")
        tmdb_df = pd.read_parquet(self.tmdb_norm)
        mb_path = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        if not mb_path.exists() or os.path.getsize(mb_path) == 0:
            self.logger.warning(f"🪫 MB candidate file missing or empty → {mb_path}")
            return
        mb_df = pd.read_csv(mb_path, dtype=str)

        if self.sample and self.sample < len(tmdb_df):
            tmdb_df = tmdb_df.sample(self.sample, random_state=42)
            self.logger.info(f"🔬 Using sample of {self.sample:,} TMDB rows for testing.")

        # --- Data validation ---
        self.logger.info(f"TMDB shape={tmdb_df.shape}, MB shape={mb_df.shape}")
        for df, name in [(tmdb_df, "TMDB"), (mb_df, "MB")]:
            nulls = df[["title", "year"]].isna().sum().to_dict()
            self.logger.info(f"{name} null counts: {nulls}")

        tmdb_df = self._ensure_normalized(tmdb_df, "title")
        mb_df = self._ensure_normalized(mb_df, "title")

        tmdb_df["year"] = pd.to_numeric(tmdb_df["year"], errors="coerce")
        mb_df["year"] = pd.to_numeric(mb_df["year"], errors="coerce")
        mb_df = mb_df.dropna(subset=["normalized_title"])  # keep rows even if year is NaN/-1

        tmdb_df = tmdb_df.sort_values(["tmdb_id", "year"]).reset_index(drop=True)
        mb_df = mb_df.sort_values(["release_group_id", "year"]).reset_index(drop=True)

        # --- Exact matches ---
        exact = tmdb_df.merge(
            mb_df[["normalized_title", "year", "title", "release_group_id"]],
            on=["normalized_title", "year"],
            how="inner",
            suffixes=("_tmdb", "_mb"),
        )
        exact_matches = [
            {
                "tmdb_id": r.tmdb_id,
                "tmdb_title": r.title_tmdb,
                "tmdb_year": r.year,
                "matched_title": r.title_mb,
                "release_group_id": r.release_group_id,
                "mb_year": r.year,
                "score": 100,
                "match_mode": "exact",
            }
            for r in exact.itertuples(index=False)
        ]
        self.logger.info(f"✅ Exact matches: {len(exact_matches):,}")

        remaining = tmdb_df[~tmdb_df["tmdb_id"].isin(exact["tmdb_id"])]
        norm_to_mb = {
            r.normalized_title: (r.title, r.release_group_id, r.year)
            for r in mb_df.itertuples(index=False)
        }

        matches, misses = list(exact_matches), []
        alt_rescue_count, no_candidate_count, no_year_count = 0, 0, 0

        # --- Fuzzy matching (bounded + plausibility) ---
        for mv in self.progress_iter(remaining.itertuples(index=False), desc="Fuzzy Matching"):
            q_norm, q_year, q_title = mv.normalized_title, mv.year, mv.title

            # Skip rows with unknown TMDb year — we can't bound
            if pd.isna(q_year):
                no_year_count += 1
                misses.append({"tmdb_id": mv.tmdb_id, "tmdb_title": q_title, "tmdb_year": None, "reason": "tmdb_no_year"})
                continue

            # Wider of configured YEAR_VARIANCE or 3; fallback bounded to ±10
            primary_window = max(int(YEAR_VARIANCE), 3)
            pool_df = mb_df[mb_df["year"].between(q_year - primary_window, q_year + primary_window)]
            if pool_df.empty:
                pool_df = mb_df[mb_df["year"].between(q_year - 10, q_year + 10)]

            if pool_df.empty:
                no_candidate_count += 1
                misses.append({"tmdb_id": mv.tmdb_id, "tmdb_title": q_title, "tmdb_year": int(q_year), "reason": "no_candidates"})
                continue

            pool = pool_df["normalized_title"].tolist()
            best_n = process.extract(q_norm, pool, scorer=fuzz.WRatio, limit=TOP_N)
            best_cand, best_score = (best_n[0][0], best_n[0][1]) if best_n else ("", 0)
            match_mode = "fuzzy"

            # Alt-title rescue
            if best_score < FUZZ_THRESHOLD and USE_ALT_TITLES:
                for alt in fetch_alt_titles(mv.tmdb_id):
                    alt_n = process.extract(alt, pool, scorer=fuzz.WRatio, limit=TOP_N)
                    if alt_n and alt_n[0][1] > best_score:
                        best_cand, best_score = alt_n[0][0], alt_n[0][1]
                        match_mode = "alt_rescue"
                        alt_rescue_count += 1

            # Plausibility guard: require at least one 4+ char token overlap
            mb_title, rgid, mb_year = norm_to_mb.get(best_cand, ("", "", None))
            if best_score >= FUZZ_THRESHOLD:
                tset, mset = self._wordset(q_title), self._wordset(mb_title)
                if not (tset & mset):
                    # implausible lexical overlap → treat as miss
                    misses.append({
                        "tmdb_id": mv.tmdb_id,
                        "tmdb_title": q_title,
                        "tmdb_year": int(q_year),
                        "best_candidate": mb_title,
                        "score": int(best_score),
                        "reason": "implausible_overlap",
                    })
                else:
                    matches.append({
                        "tmdb_id": mv.tmdb_id,
                        "tmdb_title": q_title,
                        "tmdb_year": int(q_year),
                        "matched_title": mb_title,
                        "release_group_id": rgid,
                        "mb_year": mb_year,
                        "score": int(best_score),
                        "match_mode": match_mode,
                    })
            else:
                misses.append({
                    "tmdb_id": mv.tmdb_id,
                    "tmdb_title": q_title,
                    "tmdb_year": int(q_year),
                    "best_candidate": mb_title,
                    "score": int(best_score),
                    "reason": "below_threshold",
                })

            if DEBUG_MODE:
                self.logger.info(
                    f"[DEBUG] {q_title} ({int(q_year) if not pd.isna(q_year) else 'NA'}) → {mb_title} [{best_score}] mode={match_mode}"
                )

        # --- Outputs ---
        self.safe_overwrite(pd.DataFrame(matches), self.output_matches)
        self.safe_overwrite(pd.DataFrame(misses), self.output_unmatched)
        pd.DataFrame(matches).to_parquet(self.output_parquet, index=False)

        # Score histogram
        score_hist = (
            pd.DataFrame(
                pd.cut(pd.Series([m.get("score", 0) for m in matches]), bins=np.arange(0, 105, 5))
                .value_counts()
                .sort_index()
            )
            .reset_index()
            .rename(columns={"index": "score_bin", 0: "count"})
        )
        self.safe_overwrite(score_hist, TMDB_DIR / "tmdb_fuzzy_score_histogram.csv")

        # Metrics
        fuzzy_only = len(matches) - len([m for m in matches if m.get("match_mode") == "exact"])
        total_tmdb = len(tmdb_df)
        fuzzy_pct = (len(matches) / total_tmdb * 100) if total_tmdb else 0
        avg_score = pd.DataFrame(matches)["score"].mean() if matches else 0
        median_score = pd.DataFrame(matches)["score"].median() if matches else 0

        metrics = {
            "rows_tmdb": total_tmdb,
            "rows_matched_total": len(matches),
            "rows_matched_exact": len([m for m in matches if m.get("match_mode") == "exact"]),
            "rows_matched_fuzzy_only": fuzzy_only,
            "overall_match_pct": round(fuzzy_pct, 2),
            "avg_match_score": round(avg_score, 2) if avg_score == avg_score else 0,  # guard NaN
            "median_match_score": round(median_score, 2) if median_score == median_score else 0,
            "rows_alt_rescue": alt_rescue_count,
            "rows_no_candidates": no_candidate_count,
            "rows_tmdb_no_year": no_year_count,
            "source_dir": str(TMDB_DIR),
        }
        self.write_metrics("step08_match_tmdb", metrics)
        self.logger.info(f"📈 Metrics logged: {metrics}")
        self.logger.info("🎬 [DONE] Step 08 (Instrumented, Refactored) completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Step 08: Match TMDb with optional sampling")
    parser.add_argument("--sample", type=int, default=None, help="Number of TMDB rows to process (for testing)")
    args = parser.parse_args()

    Step08MatchTMDb(sample=args.sample).run()
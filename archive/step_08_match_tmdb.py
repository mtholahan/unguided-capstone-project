"""
Step 08: Match TMDb (Hardened + DRY)
------------------------------------
Uses utils.normalize_for_matching_extended() when needed, skips fuzzy for exact
matches (title+year), and optionally uses TMDB alternative titles as a fallback.

Inputs:
    TMDB_DIR/tmdb_movies_normalized.parquet      (from Step 07)
    TMDB_DIR/tmdb_input_candidates_clean.csv     (from Step 07)
Outputs:
    TMDB_DIR/tmdb_match_results_enhanced.csv
    TMDB_DIR/tmdb_match_unmatched.csv
    TMDB_DIR/tmdb_match_results_enhanced.parquet
Metrics:
    fuzzy_match_pct, avg_fuzzy_score → pipeline_metrics.csv
"""

from base_step import BaseStep
import pandas as pd, requests
from rapidfuzz import fuzz, process
from config import TMDB_DIR, DEBUG_MODE, TMDB_API_KEY
from utils import normalize_for_matching_extended as normalize
from tqdm import tqdm
import os

# ---- Tunables ----
YEAR_VARIANCE = 5
FUZZ_THRESHOLD = 50
TOP_N = 5
FORCE_RENORM = False        # Recompute normalized_title from 'title' even if present
USE_ALT_TITLES = True       # Query TMDB alt titles to rescue borderline cases (slower)

# 🧠 GPT Suggestion:
#Leave FORCE_RENORM=False for speed. Flip to True if you tweak utils.py and want Step 08 to re-derive normalized_title fresh.
#Leave USE_ALT_TITLES=True only if you’re okay with API calls (slower but helpful for foreign/AKA titles).

def fetch_alt_titles(tmdb_id: str) -> list[str]:
    if not USE_ALT_TITLES or not TMDB_API_KEY:
        return []
    try:
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/alternative_titles"
        r = requests.get(url, params={"api_key": TMDB_API_KEY})
        r.raise_for_status()
        titles = [t.get("title", "") for t in r.json().get("titles", []) if t.get("title")]
        return [normalize(t) for t in titles]
    except Exception:
        return []


class Step08MatchTMDb(BaseStep):
    def __init__(self, name="Step 08: Match TMDb (Hardened)"):
        super().__init__(name=name)
        self.tmdb_norm = TMDB_DIR / "tmdb_movies_normalized.parquet"
        self.mb_candidates = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        self.output_matches = TMDB_DIR / "tmdb_match_results_enhanced.csv"
        self.output_unmatched = TMDB_DIR / "tmdb_match_unmatched.csv"
        self.output_parquet = TMDB_DIR / "tmdb_match_results_enhanced.parquet"

    def _ensure_normalized(self, df: pd.DataFrame, title_col: str) -> pd.DataFrame:
        needs = FORCE_RENORM or ("normalized_title" not in df.columns) or df["normalized_title"].isna().any()
        if needs:
            self.logger.info("🧼 (Re)normalizing titles via utils.normalize_for_matching_extended()")
            df["normalized_title"] = df[title_col].fillna("").map(normalize)
        return df

    def run(self):
        # -- Load inputs
        self.logger.info("📥 Loading normalized TMDB + MB datasets...")
        tmdb_df = pd.read_parquet(self.tmdb_norm)
        mb_df = pd.read_csv(self.mb_candidates, dtype=str)

        # --- Load MB candidates safely ---
        mb_path = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        if not mb_path.exists() or os.path.getsize(mb_path) == 0:
            self.logger.warning(f"🪫 MB candidate file missing or empty → {mb_path}")
            return

        mb_df = pd.read_csv(mb_path, dtype=str)
        # Defensive conversions
        mb_df["year"] = pd.to_numeric(mb_df.get("year", None), errors="coerce")
        mb_df["normalized_title"] = mb_df["normalized_title"].fillna("").astype(str)
        mb_df = mb_df.dropna(subset=["normalized_title"])
        self.logger.info(f"📥 Loaded MB candidates: {len(mb_df):,} rows")

        # -- Safety: normalized_title availability
        tmdb_df = self._ensure_normalized(tmdb_df, "title")
        mb_df = self._ensure_normalized(mb_df, "title")

        # -- Types
        tmdb_df["year"] = pd.to_numeric(tmdb_df["year"], errors="coerce")
        mb_df["year"]   = pd.to_numeric(mb_df["year"], errors="coerce")

        # -- Exact (deterministic) matches by normalized_title + year
        exact = tmdb_df.merge(
            mb_df[["normalized_title", "year", "title", "release_group_id"]],
            on=["normalized_title", "year"], how="inner", suffixes=("_tmdb", "_mb")
        )
        exact_matches = [{
            "tmdb_id": r.tmdb_id,
            "tmdb_title": r.title_tmdb,
            "tmdb_year": r.year,
            "matched_title": r.title_mb,
            "release_group_id": r.release_group_id,
            "mb_year": r.year,
            "score": 100,  # exact deterministic
        } for r in exact.itertuples(index=False)]

        self.logger.info(f"✅ Exact matches (pre-fuzzy): {len(exact_matches)}")

        # -- Fuzzy only for remaining TMDB rows
        remaining = tmdb_df[~tmdb_df["tmdb_id"].isin(exact["tmdb_id"])]
        self.logger.info(f"🎯 Remaining for fuzzy: {len(remaining)} (of {len(tmdb_df)})")

        # Pre-compute MB lookup and per-year pools
        norm_to_mb = {}
        for r in mb_df.itertuples(index=False):
            # In case of collisions, first one wins (Step 07 already deduped per title+year)
            norm_to_mb.setdefault(r.normalized_title, (r.title, r.release_group_id, r.year))

        matches, misses = list(exact_matches), []

        with tqdm(total=len(remaining), desc="Fuzzy Matching") as bar:
            for mv in remaining.itertuples(index=False):
                q_norm, q_year = mv.normalized_title, mv.year

                # Candidate pool: MB within year ± tolerance
                pool_df = mb_df[mb_df["year"].between(q_year - YEAR_VARIANCE, q_year + YEAR_VARIANCE)]
                pool = pool_df["normalized_title"].tolist()

                if not pool:
                    misses.append({"tmdb_id": mv.tmdb_id, "tmdb_title": mv.title, "tmdb_year": q_year, "reason": "no_candidates"})
                    bar.update(1); continue

                # Primary fuzzy
                best_n = process.extract(q_norm, pool, scorer=lambda a,b,**_: int(0.7*fuzz.token_set_ratio(a,b) + 0.3*fuzz.partial_ratio(a,b)), limit=TOP_N, processor=None)
                best_cand, best_score = (best_n[0][0], best_n[0][1]) if best_n else ("", 0)

                # Optional alt-title rescue
                if best_score < FUZZ_THRESHOLD and USE_ALT_TITLES:
                    for alt in fetch_alt_titles(mv.tmdb_id):
                        alt_n = process.extract(alt, pool, scorer=lambda a,b,**_: int(0.7*fuzz.token_set_ratio(a,b) + 0.3*fuzz.partial_ratio(a,b)), limit=TOP_N, processor=None)
                        if alt_n and alt_n[0][1] > best_score:
                            best_cand, best_score = alt_n[0][0], alt_n[0][1]

                if best_score >= FUZZ_THRESHOLD:
                    mb_title, rgid, mb_year = norm_to_mb.get(best_cand, ("", "", None))
                    matches.append({
                        "tmdb_id": mv.tmdb_id, "tmdb_title": mv.title,
                        "tmdb_year": q_year, "matched_title": mb_title,
                        "release_group_id": rgid, "mb_year": mb_year,
                        "score": int(best_score),
                    })
                else:
                    misses.append({
                        "tmdb_id": mv.tmdb_id, "tmdb_title": mv.title,
                        "tmdb_year": q_year, "best_candidate": best_cand,
                        "score": int(best_score),
                    })

                if DEBUG_MODE:
                    self.logger.info(f"[DEBUG] {mv.title} ({q_year}) → {best_cand} [{best_score}]")
                bar.update(1)

            if not matches:
                self.logger.warning("🪫 No fuzzy matches met the threshold. Try lowering FUZZ_THRESHOLD or verify normalization.")

        # -- Save outputs
        pd.DataFrame(matches).to_csv(self.output_matches, index=False)
        pd.DataFrame(misses).to_csv(self.output_unmatched, index=False)
        pd.DataFrame(matches).to_parquet(self.output_parquet, index=False)
        self.logger.info(f"📦 Saved {len(matches)} matches, {len(misses)} unmatched")

        # -- Metrics
        fuzzy_only = len(matches) - len(exact_matches)
        total_tmdb = len(tmdb_df)
        fuzzy_pct = (len(matches) / total_tmdb * 100) if total_tmdb else 0
        avg_score = pd.DataFrame(matches)["score"].mean() if matches else 0
        metrics = {
            "rows_tmdb": total_tmdb,
            "rows_matched_total": len(matches),
            "rows_matched_exact": len(exact_matches),
            "rows_matched_fuzzy_only": fuzzy_only,
            "overall_match_pct": round(fuzzy_pct, 2),
            "avg_match_score": round(avg_score, 2),
        }
        self.write_metrics("step08_match_tmdb", metrics)
        self.logger.info(f"📈 Metrics logged → Power BI ({metrics})")
        self.logger.info("🎬 Step 08 completed successfully.")


if __name__ == "__main__":
    Step08MatchTMDb().run()

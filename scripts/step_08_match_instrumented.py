"""
Step 08: Match TMDb (Instrumented)
----------------------------------
Performs exact + fuzzy matching between normalized TMDb and MusicBrainz datasets.
Uploads results and metrics to Azure Blob (optional). Fully instrumented for monitoring.
"""

from base_step import BaseStep
import pandas as pd, requests, numpy as np, os, argparse
from rapidfuzz import fuzz, process
from config import TMDB_DIR, DEBUG_MODE, TMDB_API_KEY, AZURE_CONN_STR, BLOB_CONTAINER
from utils import normalize_for_matching_extended as normalize, make_progress_bar  # ✅ unified helper
from azure.storage.blob import BlobServiceClient
from pathlib import Path

FUZZ_THRESHOLD = 55
TOP_N = 5
FORCE_RENORM = False
USE_ALT_TITLES = True
YEAR_VARIANCE = 2  # moved from config


# ============================================================
# Helper: Azure-safe overwrite + optional upload
# ============================================================
class SafeWriterMixin:
    def safe_overwrite(self, df: pd.DataFrame, path: Path, upload_to_blob=False):
        """Write CSV atomically, then optionally push to Azure Blob."""
        tmp = Path(f"{path}.tmp")
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
        self.logger.info(f"💾 Wrote {len(df):,} rows → {path.name}")

        if upload_to_blob:
            self.upload_to_blob(path)

    def upload_to_blob(self, local_path: Path):
        """Upload a local file to Azure Blob Storage."""
        if not AZURE_CONN_STR:
            self.logger.info("Skipping Azure upload (no connection string set).")
            return
        try:
            blob_service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
            blob_client = blob_service.get_blob_client(
                container=BLOB_CONTAINER,
                blob=f"outputs/{local_path.name}"
            )
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            self.logger.info(f"☁️ Uploaded {local_path.name} → blob:{BLOB_CONTAINER}/outputs/")
        except Exception as e:
            self.logger.warning(f"⚠️ Azure upload skipped or failed: {e}")


def fetch_alt_titles(tmdb_id: str) -> list[str]:
    """Fetch alternative titles for a TMDb movie."""
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


# ============================================================
# Step 08: Match TMDb (Instrumented)
# ============================================================
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

    def run(self):
        self.logger.info("📥 Loading normalized TMDB + MB datasets...")
        tmdb_df = pd.read_parquet(self.tmdb_norm)
        mb_path = TMDB_DIR / "tmdb_input_candidates_clean.csv"

        if not mb_path.exists() or os.path.getsize(mb_path) == 0:
            self.logger.warning(f"🪫 MB candidate file missing or empty → {mb_path}")
            return

        mb_df = pd.read_csv(mb_path, dtype=str)

        # Sampling for quick test runs
        if self.sample and self.sample < len(tmdb_df):
            tmdb_df = tmdb_df.sample(self.sample, random_state=42)
            self.logger.info(f"🔬 Using sample of {self.sample} TMDB rows for testing.")

        # --- Data validation ---
        self.logger.info(f"TMDB shape={tmdb_df.shape}, MB shape={mb_df.shape}")
        for df, name in [(tmdb_df, "TMDB"), (mb_df, "MB")]:
            nulls = df[['title', 'year']].isna().sum().to_dict()
            self.logger.info(f"{name} null counts: {nulls}")

        tmdb_df = self._ensure_normalized(tmdb_df, 'title')
        mb_df = self._ensure_normalized(mb_df, 'title')

        tmdb_df['year'] = pd.to_numeric(tmdb_df['year'], errors='coerce')
        mb_df['year'] = pd.to_numeric(mb_df['year'], errors='coerce')
        mb_df = mb_df.dropna(subset=['normalized_title'])

        tmdb_df = tmdb_df.sort_values(['tmdb_id', 'year']).reset_index(drop=True)
        mb_df = mb_df.sort_values(['release_group_id', 'year']).reset_index(drop=True)

        # --- Exact match ---
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
            "score": 100,
            "match_mode": "exact",
        } for r in exact.itertuples(index=False)]

        self.logger.info(f"✅ Exact matches: {len(exact_matches):,}")

        remaining = tmdb_df[~tmdb_df['tmdb_id'].isin(exact['tmdb_id'])]
        norm_to_mb = {r.normalized_title: (r.title, r.release_group_id, r.year) for r in mb_df.itertuples(index=False)}

        matches, misses = list(exact_matches), []
        alt_rescue_count = 0
        no_candidate_count = 0

        # ✅ Unified progress bar for fuzzy matching
        with make_progress_bar(total=len(remaining), desc="Fuzzy Matching", unit="movie") as bar:
            for mv in remaining.itertuples(index=False):
                q_norm, q_year = mv.normalized_title, mv.year
                pool_df = mb_df[mb_df['year'].between(q_year - YEAR_VARIANCE, q_year + YEAR_VARIANCE)]
                pool = pool_df['normalized_title'].tolist()

                if not pool:
                    misses.append({"tmdb_id": mv.tmdb_id, "tmdb_title": mv.title, "tmdb_year": q_year, "reason": "no_candidates"})
                    no_candidate_count += 1
                    bar.update(1)
                    continue

                best_n = process.extract(
                    q_norm, pool,
                    scorer=lambda a, b, **_: int(0.7 * fuzz.token_set_ratio(a, b) + 0.3 * fuzz.partial_ratio(a, b)),
                    limit=TOP_N, processor=None
                )
                best_cand, best_score = (best_n[0][0], best_n[0][1]) if best_n else ("", 0)
                match_mode = "fuzzy"

                if best_score < FUZZ_THRESHOLD and USE_ALT_TITLES:
                    for alt in fetch_alt_titles(mv.tmdb_id):
                        alt_n = process.extract(
                            alt, pool,
                            scorer=lambda a, b, **_: int(0.7 * fuzz.token_set_ratio(a, b) + 0.3 * fuzz.partial_ratio(a, b)),
                            limit=TOP_N, processor=None
                        )
                        if alt_n and alt_n[0][1] > best_score:
                            best_cand, best_score = alt_n[0][0], alt_n[0][1]
                            match_mode = "alt_rescue"
                            alt_rescue_count += 1

                if best_score >= FUZZ_THRESHOLD:
                    mb_title, rgid, mb_year = norm_to_mb.get(best_cand, ("", "", None))
                    matches.append({
                        "tmdb_id": mv.tmdb_id,
                        "tmdb_title": mv.title,
                        "tmdb_year": q_year,
                        "matched_title": mb_title,
                        "release_group_id": rgid,
                        "mb_year": mb_year,
                        "score": int(best_score),
                        "match_mode": match_mode,
                    })
                else:
                    misses.append({
                        "tmdb_id": mv.tmdb_id,
                        "tmdb_title": mv.title,
                        "tmdb_year": q_year,
                        "best_candidate": best_cand,
                        "score": int(best_score),
                    })

                if DEBUG_MODE:
                    self.logger.info(f"[DEBUG] {mv.title} ({q_year}) → {best_cand} [{best_score}] mode={match_mode}")
                bar.update(1)

        # --- Outputs ---
        self.safe_overwrite(pd.DataFrame(matches), self.output_matches)
        self.safe_overwrite(pd.DataFrame(misses), self.output_unmatched)
        pd.DataFrame(matches).to_parquet(self.output_parquet, index=False)

        score_hist = (
            pd.DataFrame(
                pd.cut(pd.Series([m["score"] for m in matches]), bins=np.arange(0, 105, 5))
                .value_counts()
                .sort_index()
            )
            .reset_index()
            .rename(columns={"index": "score_bin", 0: "count"})
        )
        self.safe_overwrite(score_hist, TMDB_DIR / "tmdb_fuzzy_score_histogram.csv")

        fuzzy_only = len(matches) - len(exact_matches)
        total_tmdb = len(tmdb_df)
        fuzzy_pct = (len(matches) / total_tmdb * 100) if total_tmdb else 0
        avg_score = pd.DataFrame(matches)["score"].mean() if matches else 0
        median_score = pd.DataFrame(matches)["score"].median() if matches else 0

        metrics = {
            "rows_tmdb": total_tmdb,
            "rows_matched_total": len(matches),
            "rows_matched_exact": len(exact_matches),
            "rows_matched_fuzzy_only": fuzzy_only,
            "overall_match_pct": round(fuzzy_pct, 2),
            "avg_match_score": round(avg_score, 2),
            "median_match_score": round(median_score, 2),
            "rows_alt_rescue": alt_rescue_count,
            "rows_no_candidates": no_candidate_count,
        }

        self.write_metrics("step08_match_tmdb", metrics)
        self.logger.info(f"📈 Metrics logged: {metrics}")
        self.logger.info("🎬 Step 08 (Instrumented) completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Step 08: Match TMDb with optional sampling")
    parser.add_argument("--sample", type=int, default=None, help="Number of TMDB rows to process (for testing)")
    args = parser.parse_args()

    Step08MatchTMDb(sample=args.sample).run()

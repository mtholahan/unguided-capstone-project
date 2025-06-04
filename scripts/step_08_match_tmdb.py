import os
import pandas as pd
import requests
from rapidfuzz import fuzz, process
from base_step import BaseStep
from config import TMDB_DIR, TMDB_API_KEY, YEAR_VARIANCE
from utils import normalize_title_for_matching

# Helper to fetch alternative titles via TMDb API
def fetch_alt_titles(tmdb_id: str) -> list[str]:
    """
    Fetch alternative titles from TMDb. Returns a list of raw title strings.
    """
    try:
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/alternative_titles"
        r = requests.get(url, params={"api_key": TMDB_API_KEY})
        r.raise_for_status()
        data = r.json().get("titles", [])
        return [t["title"] for t in data if t.get("title")]
    except Exception:
        return []

class Step08MatchTMDb(BaseStep):
    def __init__(
        self,
        name: str = "Step 08: Match TMDb Titles",
        threshold: float = 65.0,
        enable_vector_matching: bool = False
    ):
        super().__init__(name)
        self.threshold = threshold
        self.enable_vector = enable_vector_matching

        # Input/Output paths
        self.input_movies       = TMDB_DIR / "enriched_top_1000.csv"
        self.input_candidates   = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        self.output_matches     = TMDB_DIR / "tmdb_match_results.csv"
        self.output_unmatched   = TMDB_DIR / "tmdb_match_unmatched.csv"
        self.manual_rescue_path = TMDB_DIR / "manual_rescue.csv"

        # Uncomment to enable SBERT fallback
        # try:
        #     from sentence_transformers import SentenceTransformer, util
        #     self.model = SentenceTransformer("all-MiniLM-L6-v2")
        #     self.util  = util
        #     self.enable_vector = True
        # except ImportError:
        #     self.logger.info("ℹ️ SBERT not installed → skipping vector fallback")
        #     self.enable_vector = False

    def normalize_title(self, title: str) -> str:
        txt = str(title) if not pd.isna(title) else ""
        return " ".join(txt.lower().split())

    def run(self):
        # 1) Load & validate TMDb movies
        self.logger.info("🎬 Loading TMDb movie titles…")
        movies_df = pd.read_csv(self.input_movies, dtype={"tmdb_id": str})
        if "tmdb_id" not in movies_df.columns:
            raise KeyError(f"Missing 'tmdb_id' in {self.input_movies}: {movies_df.columns.tolist()}")
        movies_df["normalized_title"] = movies_df["title"].apply(normalize_title_for_matching)
        total = len(movies_df)

        # 2) Load & validate candidate soundtracks
        self.logger.info("🎧 Loading soundtrack candidates…")
        cands_df = pd.read_csv(self.input_candidates, dtype={"release_group_id": str})
        required_cols = {"release_group_id", "title", "year", "normalized_title"}
        if not required_cols.issubset(set(cands_df.columns)):
            raise KeyError(f"Candidates missing cols {required_cols}, found: {cands_df.columns.tolist()}")

        all_norms = cands_df["normalized_title"].tolist()
        norm_to_raw = {
            row.normalized_title: (row.title, row.release_group_id)
            for row in cands_df.itertuples(index=False)
        }

        # 3) Matching loop
        self.logger.info(
            f"🔍 Matching {total} movies (threshold={self.threshold}, year ±{YEAR_VARIANCE}) with composite fuzzy…"
        )
        matches, misses = [], []

        for idx, mv in enumerate(movies_df.itertuples(index=False), start=1):
            if idx % 50 == 0 or idx == total:
                print(f"➤ Processed {idx}/{total}", flush=True)

            tmdb_id    = mv.tmdb_id
            base_norm  = mv.normalized_title
            tmdb_year  = getattr(mv, "release_year", None)

            # 3a) Year filter ±YEAR_VARIANCE
            if tmdb_year is not None and "year" in cands_df.columns:
                mask = cands_df["year"].between(tmdb_year - YEAR_VARIANCE, tmdb_year + YEAR_VARIANCE)
                pool_norms = cands_df.loc[mask, "normalized_title"].tolist()
            else:
                pool_norms = all_norms

            # Composite fuzzy scorer (accept **kwargs for RapidFuzz)
            def composite_scorer(query: str, candidate: str, **kwargs):
                s1 = fuzz.token_set_ratio(query, candidate)
                s2 = fuzz.token_sort_ratio(query, candidate)
                s3 = fuzz.partial_ratio(query, candidate)
                return max(s1, s2, s3)

            best = process.extractOne(
                base_norm,
                pool_norms,
                scorer=composite_scorer
            )
            if best is None:
                score, best_norm = -1, None
            else:
                best_norm, score, _ = best

            # 3b) Fallback to TMDb alt titles if below threshold
            if score < self.threshold:
                alt_titles = fetch_alt_titles(tmdb_id)
                for alt in [normalize_title_for_matching(a) for a in alt_titles]:
                    alt_best = process.extractOne(alt, pool_norms, scorer=composite_scorer)
                    if alt_best:
                        cand_norm, alt_score, _ = alt_best
                        if alt_score > score:
                            score, best_norm = alt_score, cand_norm
                            if score >= self.threshold:
                                break

            # 3c) (Optional) SBERT fallback
            # if score < self.threshold and self.enable_vector:
            #     emb1 = self.model.encode(mv.title, convert_to_tensor=True)
            #     emb2 = self.model.encode(best_norm or "", convert_to_tensor=True)
            #     vec_score = float(self.util.pytorch_cos_sim(emb1, emb2)[0][0] * 100)
            #     if vec_score > score:
            #         score = vec_score

            # 3d) Map back to raw title & release_group_id
            if best_norm:
                raw_title, rgid = norm_to_raw.get(best_norm, (None, None))
            else:
                raw_title, rgid = None, None

            # 3e) Record match or miss
            if score >= self.threshold:
                matches.append({
                    "tmdb_id": tmdb_id,
                    "tmdb_title": mv.title,
                    "matched_title": raw_title,
                    "score": score,
                    "release_group_id": rgid
                })
            else:
                misses.append({"tmdb_title": mv.title, "best_match": raw_title, "score": score})

        # 4) Save outputs
        matches_df = pd.DataFrame(matches)
        matches_df.to_csv(self.output_matches, index=False)
        self.logger.info(f"✅ Saved {len(matches_df)} matches to {self.output_matches}")

        if misses:
            self.logger.info("📉 Top 10 near misses:")
            for e in sorted(misses, key=lambda x: -x["score"])[:10]:
                self.logger.info(f"{e['tmdb_title']} → {e['best_match']} ({e['score']})")
        unmatched_df = pd.DataFrame(misses)
        unmatched_df.to_csv(self.output_unmatched, index=False)
        self.logger.info(f"📁 Saved {len(unmatched_df)} unmatched to {self.output_unmatched}")

        # 5) Manual rescue merge
        try:
            mr = pd.read_csv(self.manual_rescue_path, sep="\t", dtype=str)
            merged = pd.concat([matches_df, mr], ignore_index=True)
            merged.to_csv(self.output_matches, index=False)
            self.logger.info(f"🛠️ Merged manual rescues → {len(merged)} total")
        except FileNotFoundError:
            self.logger.info("🪫 No manual_rescue.csv found; skipping rescue")
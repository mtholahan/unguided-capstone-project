import os
import pandas as pd
import requests
from rapidfuzz import fuzz, process
from base_step import BaseStep
from config import DATA_DIR

# If you want TMDb‐API alt‐titles like the legacy script, set your key here:
TMDB_API_KEY = os.getenv("TMDB_API_KEY")

def fetch_alt_titles(tmdb_id: int) -> list[str]:
    """
    Fetch alternative titles from TMDb. Returns a list of cleaned titles.
    Returns [] on any error.
    """
    try:
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/alternative_titles"
        r = requests.get(url, params={"api_key": TMDB_API_KEY})
        if r.status_code == 200:
            data = r.json().get("titles", [])
            # Extract only the 'title' field, drop duplicates
            return [t["title"] for t in data if t.get("title")]
    except Exception:
        pass
    return []


class Step07MatchTMDb(BaseStep):
    def __init__(
        self,
        name: str = "Step 07: Match TMDb Titles",
        threshold: float = 65.0,
        enable_vector_matching: bool = False,  # If you want SBERT fallback (slow)
    ):
        super().__init__(name)
        self.threshold = threshold
        self.enable_vector = enable_vector_matching

        # Input/Output paths
        self.input_movies       = DATA_DIR / "tmdb" / "enriched_top_1000.csv"
        self.input_candidates   = DATA_DIR / "tmdb_input_candidates_clean.csv"
        self.output_matches     = DATA_DIR / "tmdb_match_results.csv"
        self.output_unmatched   = DATA_DIR / "tmdb_match_unmatched.csv"
        self.manual_rescue_path = DATA_DIR / "manual_rescue.csv"

        # If you do want SBERT fallback, uncomment below and install sentence-transformers
        # try:
        #     from sentence_transformers import SentenceTransformer, util
        #     self.model = SentenceTransformer("all-MiniLM-L6-v2")
        #     self.util  = util
        #     self.enable_vector = True
        # except ImportError:
        #     self.logger.info("ℹ️ SBERT not installed → skipping vector fallback")
        #     self.enable_vector = False

    def normalize_title(self, title: str) -> str:
        """
        Lowercase, collapse whitespace, return safe string even if NaN.
        """
        txt = str(title) if not pd.isna(title) else ""
        return " ".join(txt.lower().split())

    def run(self):
        # ────────────────────
        # 1) Load & preprocess
        # ────────────────────
        self.logger.info("🎬 Loading TMDb movie titles…")
        movies_df = pd.read_csv(self.input_movies, dtype={"tmdb_id": str})
        if "tmdb_id" not in movies_df.columns:
            raise KeyError(f"Missing 'tmdb_id' in {self.input_movies}: {movies_df.columns.tolist()}")
        # Normalize TMDb titles
        movies_df["normalized_title"] = movies_df["title"].apply(self.normalize_title)

        total = len(movies_df)

        self.logger.info("🎧 Loading soundtrack candidates…")
        cands_df = pd.read_csv(self.input_candidates, dtype={"release_group_id": str})
        required_cols = {"release_group_id", "title", "year", "normalized_title"}
        if not required_cols.issubset(set(cands_df.columns)):
            raise KeyError(f"Candidates missing cols {required_cols}, found: {cands_df.columns.tolist()}")

        # Prepare two pre-built lists for fast lookups:
        #   1) `all_norms` = list of every candidate.normalized_title
        #   2) `norm_to_raw` = dict(normalized_title -> (raw_title, release_group_id, year))
        all_norms = cands_df["normalized_title"].tolist()
        norm_to_raw = {
            row.normalized_title: (row.title, row.release_group_id, row.year)
            for row in cands_df.itertuples(index=False)
        }

        # ────────────────────
        # 2) Matching loop
        # ────────────────────
        self.logger.info(f"🔍 Matching {total} movies (threshold={self.threshold}, year ± 5) with composite‐fuzzy & fast lookups…")
        matches, misses = [], []

        for idx, mv in enumerate(movies_df.itertuples(index=False), start=1):
            # Heartbeat every 50
            if idx % 50 == 0 or idx == total:
                print(f"➤ Processed {idx}/{total}", flush=True)

            tmdb_id    = mv.tmdb_id
            base_norm  = mv.normalized_title
            tmdb_year  = getattr(mv, "release_year", None)

            # 2a) Year‐filter → build a candidate‐subset list of normalized strings
            if tmdb_year is not None:
                # pick only those candidates with year ∈ [tmdb_year−5, tmdb_year+5]
                year_mask = cands_df["year"].between(tmdb_year - 5, tmdb_year + 5)
                sub_df = cands_df[year_mask]
                pool_norms = sub_df["normalized_title"].tolist()
            else:
                pool_norms = all_norms

            # 2b) Use rapidfuzz.process.extractOne to get best fuzzy match in one call
            #     (combines token_set, token_sort, partial in a single scorer custom function)
            def composite_scorer(query: str, candidate: str):
                s1 = fuzz.token_set_ratio(query, candidate)
                s2 = fuzz.token_sort_ratio(query, candidate)
                s3 = fuzz.partial_ratio(query, candidate)
                return max(s1, s2, s3)

            best = process.extractOne(
                base_norm,              # query string
                pool_norms,             # list of normalized candidate strings
                scorer=composite_scorer # custom composite scorer
            )
            if best is None:
                # no candidates at all (unlikely if we preloaded properly)
                score, best_norm = -1, None
            else:
                best_norm, score, _ = best

            # 2c) If still below threshold, try TMDb‐API alt‐titles lookup once
            if score < self.threshold and tmdb_id:
                alt_titles = fetch_alt_titles(tmdb_id)
                # normalize each alt‐title
                alt_norms = [self.normalize_title(a) for a in alt_titles]
                # attempt fuzzy on each alt_norm
                for alt in alt_norms:
                    alt_best = process.extractOne(alt, pool_norms, scorer=composite_scorer)
                    if alt_best:
                        candidate_norm, alt_score, _ = alt_best
                        if alt_score > score:
                            score, best_norm = alt_score, candidate_norm
                            # break as soon as we exceed threshold
                            if score >= self.threshold:
                                break

            # 2d) (Optional) SBERT fallback if you want semantic similarity.
            # Uncomment only if you installed sentence-transformers; otherwise skip.
            # if score < self.threshold and self.enable_vector:
            #     emb1 = self.model.encode(mv.title,     convert_to_tensor=True)
            #     emb2 = self.model.encode(best_norm or "", convert_to_tensor=True)
            #     vec_score = float(self.util.pytorch_cos_sim(emb1, emb2)[0][0] * 100)
            #     if vec_score > score:
            #         score = vec_score
            #         # raw candidate title remains the same best_norm mapping

            # 2e) Map `best_norm` back to raw title & release_group_id
            if best_norm:
                raw_title, rgid, _yr = norm_to_raw[best_norm]
            else:
                raw_title, rgid = None, None

            # 2f) Decide match vs. miss
            if score >= self.threshold:
                matches.append({
                    "tmdb_id": tmdb_id,
                    "tmdb_title": mv.title,
                    "matched_title": raw_title,
                    "score": score,
                    "release_group_id": rgid
                })
            else:
                misses.append({
                    "tmdb_title": mv.title,
                    "best_match": raw_title,
                    "score": score
                })

        # ────────────────────
        # 3) Save Outputs
        # ────────────────────
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

        # ────────────────────
        # 4) Manual Rescue Merge
        # ────────────────────
        try:
            mr = pd.read_csv(self.manual_rescue_path)
            merged = pd.concat([matches_df, mr], ignore_index=True)
            merged.to_csv(self.output_matches, index=False)
            self.logger.info(f"🛠️ Merged manual rescues → {len(merged)} total")
        except FileNotFoundError:
            self.logger.info("🪫 No manual_rescue.csv found; skipping rescue")

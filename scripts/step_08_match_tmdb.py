"""Step 08: Match TMDb
Attempts fuzzy matching between MusicBrainz soundtracks and TMDb titles.
Writes tmdb_match_results.csv and tmdb_match_unmatched.csv to TMDB_DIR.
"""

from base_step import BaseStep
import pandas as pd, requests
from rapidfuzz import fuzz, process
from config import TMDB_DIR, TMDB_API_KEY, YEAR_VARIANCE
from utils import normalize_title_for_matching
from tqdm import tqdm


def fetch_alt_titles(tmdb_id: str) -> list[str]:
    """Fetch alternative titles from TMDb for fallback matching."""
    try:
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/alternative_titles"
        r = requests.get(url, params={"api_key": TMDB_API_KEY})
        r.raise_for_status()
        return [t["title"] for t in r.json().get("titles", []) if t.get("title")]
    except Exception:
        return []


class Step08MatchTMDb(BaseStep):
    def __init__(self, name="Step 08: Match TMDb Titles", threshold=70.0):
        super().__init__(name)
        self.threshold = threshold
        self.input_movies = TMDB_DIR / "enriched_top_1000.csv"
        self.input_candidates = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        self.output_matches = TMDB_DIR / "tmdb_match_results.csv"
        self.output_unmatched = TMDB_DIR / "tmdb_match_unmatched.csv"

    def run(self):
        # 1) Load TMDb movies
        movies_df = pd.read_csv(self.input_movies, dtype={"tmdb_id": str})
        movies_df["normalized_title"] = movies_df["title"].apply(normalize_title_for_matching)

        # 2) Load soundtrack candidates
        cands_df = pd.read_csv(self.input_candidates, dtype={"release_group_id": str})
        cands_df["normalized_title"] = cands_df["title"].apply(normalize_title_for_matching)

        required = {"release_group_id", "title", "year", "normalized_title"}
        if not required.issubset(set(cands_df.columns)):
            self.fail(f"Candidates missing required columns: {cands_df.columns.tolist()}")

        all_norms = cands_df["normalized_title"].tolist()
        norm_to_raw = {
            row.normalized_title: (row.title, row.release_group_id, row.year)
            for row in cands_df.itertuples(index=False)
        }

        def composite_scorer(q, c, **kwargs):
            return max(
                fuzz.token_set_ratio(q, c),
                fuzz.token_sort_ratio(q, c),
                fuzz.partial_ratio(q, c)
            )

        matches, misses = [], []

        # 3) Matching loop with tqdm
        total = len(movies_df)
        with tqdm(total=total, desc="Matching TMDb") as bar:
            for idx, mv in enumerate(movies_df.itertuples(index=False), start=1):
                tmdb_id, tmdb_title, tmdb_year = mv.tmdb_id, mv.title, getattr(mv, "release_year", None)
                base_norm = mv.normalized_title

                # Year filter
                if tmdb_year:
                    mask = cands_df["year"].between(tmdb_year - YEAR_VARIANCE, tmdb_year + YEAR_VARIANCE)
                    pool_norms = cands_df.loc[mask, "normalized_title"].tolist()
                else:
                    pool_norms = all_norms

                best = process.extractOne(base_norm, pool_norms, scorer=composite_scorer)
                if best:
                    best_norm, score, _ = best
                else:
                    score, best_norm = -1, None

                # Alt titles fallback
                if score < self.threshold:
                    for alt in [normalize_title_for_matching(t) for t in fetch_alt_titles(tmdb_id)]:
                        alt_best = process.extractOne(alt, pool_norms, scorer=composite_scorer)
                        if alt_best and alt_best[1] > score:
                            best_norm, score, _ = alt_best
                            if score >= self.threshold:
                                break

                if best_norm:
                    raw_title, rgid, cand_year = norm_to_raw.get(best_norm, (None, None, None))
                else:
                    raw_title, rgid, cand_year = None, None, None

                if score >= self.threshold and rgid:
                    matches.append({
                        "tmdb_id": tmdb_id,
                        "tmdb_title": tmdb_title,
                        "release_group_id": rgid,
                        "matched_title": raw_title,
                        "score": score,
                        "mb_year": cand_year,
                        "tmdb_year": tmdb_year
                    })
                else:
                    misses.append({
                        "tmdb_title": tmdb_title,
                        "best_match": raw_title,
                        "score": score
                    })

                bar.update(1)

        # 4) Save results
        pd.DataFrame(matches).to_csv(self.output_matches, index=False)
        pd.DataFrame(misses).to_csv(self.output_unmatched, index=False)
        self.logger.info(f"✅ Saved {len(matches)} matches, {len(misses)} unmatched")

"""Step 08: Match TMDb
Attempts fuzzy matching between MusicBrainz soundtracks and TMDb titles.
Writes tmdb_match_results.csv, tmdb_match_unmatched.csv, and (in Golden Test Mode) tmdb_match_golden.csv.
"""

from base_step import BaseStep
import pandas as pd, requests
from rapidfuzz import fuzz, process
from config import TMDB_DIR, TMDB_API_KEY, YEAR_VARIANCE, ROW_LIMIT, GOLDEN_TITLES, GOLDEN_TEST_MODE
from utils import normalize_title_for_matching
from tqdm import tqdm

# Debug toggles
DEBUG_MODE = True    # Enable to log top-N candidates for each TMDb movie
TOP_N = 5            # Number of candidates to log in debug mode

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

        # Input: curated TMDb movie list (Step 06 output)
        self.input_movies = TMDB_DIR / "enriched_top_1000.csv"

        # Input: cleaned MusicBrainz soundtrack candidates (Step 05 output)
        self.input_candidates = TMDB_DIR / "tmdb_input_candidates_clean.csv"

        # Output: successful matches
        self.output_matches = TMDB_DIR / "tmdb_match_results.csv"

        # Output: failed matches
        self.output_unmatched = TMDB_DIR / "tmdb_match_unmatched.csv"

        # Output: golden test set results (if enabled)
        self.output_golden = TMDB_DIR / "tmdb_match_golden.csv"

    def run(self):
        # 1) Load TMDb movies
        movies_df = pd.read_csv(self.input_movies, dtype={"tmdb_id": str})
        movies_df["normalized_title"] = movies_df["title"].apply(normalize_title_for_matching)

        # Golden test mode takes precedence
        if GOLDEN_TEST_MODE:
            before = len(movies_df)
            movies_df = movies_df[movies_df["title"].isin(GOLDEN_TITLES)].copy()
            self.logger.info(f"🔎 Golden test mode: reduced TMDb movies from {before} → {len(movies_df)}")
        elif ROW_LIMIT:
            before = len(movies_df)
            movies_df = movies_df.head(ROW_LIMIT).copy()
            self.logger.info(f"🔎 ROW_LIMIT active: reduced TMDb movies from {before} → {len(movies_df)}")

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

        matches, misses, golden_rows = [], [], []

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

                # Top-N candidates instead of single best
                best_n = process.extract(
                    base_norm,
                    pool_norms,
                    scorer=composite_scorer,
                    limit=TOP_N
                )

                if best_n:
                    best_norm, score, _ = best_n[0]
                else:
                    best_norm, score = None, -1

                # Alt titles fallback
                if score < self.threshold:
                    for alt in [normalize_title_for_matching(t) for t in fetch_alt_titles(tmdb_id)]:
                        alt_best_n = process.extract(alt, pool_norms, scorer=composite_scorer, limit=TOP_N)
                        if alt_best_n and alt_best_n[0][1] > score:
                            best_norm, score, _ = alt_best_n[0]

                if best_norm:
                    raw_title, rgid, cand_year = norm_to_raw.get(best_norm, (None, None, None))
                else:
                    raw_title, rgid, cand_year = None, None, None

                # Save match vs miss
                if score >= self.threshold and rgid:
                    row = {
                        "tmdb_id": tmdb_id,
                        "tmdb_title": tmdb_title,
                        "release_group_id": rgid,
                        "matched_title": raw_title,
                        "score": score,
                        "mb_year": cand_year,
                        "tmdb_year": tmdb_year
                    }
                    matches.append(row)
                    golden_rows.append(row)
                else:
                    row = {
                        "tmdb_id": tmdb_id,
                        "tmdb_title": tmdb_title,
                        "best_match": raw_title,
                        "score": score,
                        "tmdb_year": tmdb_year
                    }
                    misses.append(row)
                    golden_rows.append(row)

                # Debug log for this movie
                if DEBUG_MODE and best_n:
                    self.logger.info(f"[DEBUG] {tmdb_title} ({tmdb_year}) → top {TOP_N}:")
                    for cand, cand_score, _ in best_n:
                        raw, rgid, cand_year = norm_to_raw.get(cand, ("?", "?", "?"))
                        self.logger.info(f"    cand={raw[:40]!r}, score={cand_score}, year={cand_year}")

                bar.update(1)

        # 4) Save results
        pd.DataFrame(matches).to_csv(self.output_matches, index=False)
        pd.DataFrame(misses).to_csv(self.output_unmatched, index=False)
        if GOLDEN_TEST_MODE:
            pd.DataFrame(golden_rows).to_csv(self.output_golden, index=False)

        self.logger.info(f"✅ Saved {len(matches)} matches, {len(misses)} unmatched")
        if GOLDEN_TEST_MODE:
            self.logger.info(f"⭐ Golden test results saved to {self.output_golden}")

        # 5) Write fresh documentation file
        doc_text = f"""
Step 08 Output Documentation
============================

Generated by Step 08: Match TMDb Titles

Files Produced:
---------------

1. tmdb_match_results.csv
   - Contains successful matches (TMDb ↔ MB OST).
   - Schema:
       tmdb_id, tmdb_title, release_group_id, matched_title, score, mb_year, tmdb_year

2. tmdb_match_unmatched.csv
   - Contains TMDb movies that failed to meet threshold.
   - Schema:
       tmdb_title, best_match, score

3. tmdb_match_golden.csv (only if GOLDEN_TEST_MODE=True)
   - Contains both matches and misses for the ~20 iconic Golden Test films.

Inputs (for reference):
-----------------------
- enriched_top_1000.csv → TMDb movie list (Step 06 output).
- tmdb_input_candidates_clean.csv → MusicBrainz OST pool (Step 05 output).

Run Context:
------------
- Threshold: {self.threshold}
- Golden Test Mode: {GOLDEN_TEST_MODE}
- Debug Mode: {DEBUG_MODE}, Top-N={TOP_N}
- ROW_LIMIT: {ROW_LIMIT or "∞"}
"""
        doc_path = TMDB_DIR / "Step08_CSV_Documentation.txt"
        with open(doc_path, "w", encoding="utf-8") as f:
            f.write(doc_text)

        self.logger.info(f"📝 Wrote fresh documentation to {doc_path}")

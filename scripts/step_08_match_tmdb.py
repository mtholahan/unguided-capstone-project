"""Step 08: Match TMDb (Enhanced)
Improved fuzzy matching between MusicBrainz soundtracks and TMDb titles.
Adds normalization, year tolerance, artist anchoring, and refined scoring logic.
"""

from base_step import BaseStep
import pandas as pd, requests, re
from rapidfuzz import fuzz, process
from config import (
    DEBUG_MODE, TMDB_DIR, TMDB_API_KEY, YEAR_VARIANCE,
    ROW_LIMIT, GOLDEN_TITLES, GOLDEN_TEST_MODE, STEP_METRICS
)
from tqdm import tqdm

# Debug toggles
TOP_N = 5  # Number of candidates to log in debug mode

# Scoring weights
FUZZ_TOKEN_SET_WEIGHT = 0.7
FUZZ_PARTIAL_WEIGHT = 0.3

# Year and scoring modifiers
YEAR_PENALTY_PER_YEAR = 2
YEAR_PENALTY_MAX_YEARS = 3
YEAR_TOLERANCE_BONUS = 10
COMPOSER_BONUS = 8
OST_BONUS = 40
SUBSTRING_BONUS = 10
JUNK_PENALTY = -25
JUNK_TERMS = {"tribute", "karaoke", "cover", "best of", "inspired by", "volume", "vol."}
STOPWORDS = {"the", "a", "an", "of", "in", "on", "and"}

COMPOSER_HINTS = {"zimmer", "williams", "horner", "elfman", "shore", "silvestri", "newman", "doyle", "young"}


# -----------------------------------------------------
def normalize_title(title: str) -> str:
    """Aggressively normalize soundtrack titles."""
    t = str(title).lower()
    # remove OST, O.S.T., and common phrases
    t = re.sub(r"\b(o\.?s\.?t\.?|original motion picture soundtrack|music from the motion picture)\b", "", t)
    # remove bracketed text like (deluxe edition)
    t = re.sub(r"\(.*?\)|\[.*?\]", "", t)
    # collapse non-alphanumerics
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def has_token_overlap(q: str, c: str) -> bool:
    """Require at least one meaningful token overlap between query and candidate."""
    q_tokens = {tok for tok in re.split(r"[^a-z0-9]+", q.lower()) if tok and tok not in STOPWORDS}
    c_tokens = {tok for tok in re.split(r"[^a-z0-9]+", c.lower()) if tok and tok not in STOPWORDS}
    return bool(q_tokens & c_tokens)


def composite_scorer(q, c, g_year=None, c_year=None, c_type=None, c_artist=None):
    """Composite fuzzy scorer with bonuses and penalties."""
    if not has_token_overlap(q, c):
        return 0
    if any(term in c for term in JUNK_TERMS):
        return max(0, JUNK_PENALTY)

    s1 = fuzz.token_set_ratio(q, c)
    s2 = fuzz.partial_ratio(q, c)
    score = int(FUZZ_TOKEN_SET_WEIGHT * s1 + FUZZ_PARTIAL_WEIGHT * s2)

    # Year differential
    if g_year and c_year:
        try:
            dy = abs(int(g_year) - int(c_year))
            if dy <= 1:
                score += YEAR_TOLERANCE_BONUS
            else:
                score -= min(dy, YEAR_PENALTY_MAX_YEARS) * YEAR_PENALTY_PER_YEAR
        except Exception:
            pass

    # OST bonus
    if isinstance(c_type, str) and "soundtrack" in c_type.lower():
        score += OST_BONUS

    # Composer anchor bonus
    if isinstance(c_artist, str):
        artist_lower = c_artist.lower()
        if any(a in artist_lower for a in COMPOSER_HINTS):
            score += COMPOSER_BONUS

    # Substring match bonus
    if q in c or c in q:
        score += SUBSTRING_BONUS

    return score


def fetch_alt_titles(tmdb_id: str):
    """Fetch alternative titles from TMDb for fallback matching."""
    try:
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/alternative_titles"
        r = requests.get(url, params={"api_key": TMDB_API_KEY})
        r.raise_for_status()
        return [t["title"] for t in r.json().get("titles", []) if t.get("title")]
    except Exception:
        return []


def safe_best_tuple(best_list):
    """Safely extract (candidate_string, score) from RapidFuzz result tuples."""
    try:
        if best_list and isinstance(best_list[0], (list, tuple)) and len(best_list[0]) >= 2:
            return best_list[0][0], best_list[0][1]
    except Exception:
        pass
    return None, -1


def match_pool(query_norm, pool, tmdb_year, norm_to_row):
    """Reusable wrapper for RapidFuzz matching against candidate pool."""
    return process.extract(
        query_norm,
        pool["normalized_title"].tolist(),
        scorer=lambda q, c, **_: composite_scorer(
            q, c,
            tmdb_year,
            norm_to_row.get(c, (None, None, None, None, None))[2],
            norm_to_row.get(c, (None, None, None, None, None))[3],
            norm_to_row.get(c, (None, None, None, None, None))[4],
        ),
        limit=TOP_N,
    )


# -----------------------------------------------------
class Step08MatchTMDb(BaseStep):
    def __init__(self, name="Step 08: Match TMDb Titles (Enhanced)", threshold=70.0):
        super().__init__(name)
        self.threshold = threshold
        self.input_movies = TMDB_DIR / "enriched_top_1000.csv"
        self.input_candidates = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        self.output_matches = TMDB_DIR / "tmdb_match_results.csv"
        self.output_unmatched = TMDB_DIR / "tmdb_match_unmatched.csv"
        self.output_golden = TMDB_DIR / "tmdb_match_golden.csv"

    def run(self):
        # Load TMDb movies
        movies_df = pd.read_csv(self.input_movies, dtype={"tmdb_id": str})
        movies_df["normalized_title"] = movies_df["title"].apply(normalize_title)

        # Limit or Golden Mode
        if GOLDEN_TEST_MODE:
            before = len(movies_df)
            movies_df = movies_df[movies_df["title"].isin(GOLDEN_TITLES)].copy()
            self.logger.info(f"🔎 Golden test mode: {before} → {len(movies_df)} titles")
        elif ROW_LIMIT:
            before = len(movies_df)
            movies_df = movies_df.head(ROW_LIMIT).copy()
            self.logger.info(f"🔎 ROW_LIMIT active: {before} → {len(movies_df)} titles")

        # Load MB candidates
        cands_df = pd.read_csv(self.input_candidates, dtype={"release_group_id": str})
        cands_df["normalized_title"] = cands_df["title"].apply(normalize_title)

        required = {"release_group_id", "title", "year", "normalized_title", "release_group_secondary_type"}
        if not required.issubset(cands_df.columns):
            self.fail(f"Candidates missing columns: {set(required) - set(cands_df.columns)}")

        norm_to_row = {
            row.normalized_title: (row.title, row.release_group_id, row.year,
                                   getattr(row, "release_group_secondary_type", None),
                                   getattr(row, "artist", None))
            for row in cands_df.itertuples(index=False)
        }

        matches, misses, golden_rows = [], [], []
        golden_total = len([t for t in GOLDEN_TITLES if t in movies_df["title"].values])
        golden_matched = 0
        golden_with_candidates = 0

        with tqdm(total=len(movies_df), desc="Enhanced TMDb Matching") as bar:
            for mv in movies_df.itertuples(index=False):
                tmdb_id, tmdb_title = mv.tmdb_id, mv.title
                tmdb_year = getattr(mv, "release_year", None)
                base_norm = mv.normalized_title

                # Candidate filter by year ± YEAR_VARIANCE
                if tmdb_year:
                    mask = pd.to_numeric(cands_df["year"], errors="coerce").between(
                        int(tmdb_year) - YEAR_VARIANCE, int(tmdb_year) + YEAR_VARIANCE
                    )
                    pool = cands_df.loc[mask]
                else:
                    pool = cands_df

                if len(pool):
                    golden_with_candidates += 1

                # Main match
                best_n = match_pool(base_norm, pool, tmdb_year, norm_to_row)
                best_norm, score = safe_best_tuple(best_n)

                # Fallback to alt titles
                if score < self.threshold:
                    for alt in [normalize_title(t) for t in fetch_alt_titles(tmdb_id)]:
                        alt_best = match_pool(alt, pool, tmdb_year, norm_to_row)
                        alt_norm, alt_score = safe_best_tuple(alt_best)
                        if alt_score > score:
                            best_norm, score = alt_norm, alt_score

                raw_title, rgid, cand_year, cand_type, cand_artist = norm_to_row.get(
                    best_norm, (None, None, None, None, None)
                )

                pass_year_check = True
                if tmdb_year and cand_year:
                    try:
                        if abs(int(tmdb_year) - int(cand_year)) > 2:
                            pass_year_check = False
                    except Exception:
                        pass

                if score >= self.threshold and rgid and pass_year_check:
                    matches.append({
                        "tmdb_id": tmdb_id, "tmdb_title": tmdb_title,
                        "release_group_id": rgid, "matched_title": raw_title,
                        "score": score, "mb_year": cand_year,
                        "tmdb_year": tmdb_year, "mb_type": cand_type,
                        "mb_artist": cand_artist,
                    })
                    if tmdb_title in GOLDEN_TITLES:
                        golden_matched += 1
                else:
                    misses.append({
                        "tmdb_id": tmdb_id, "tmdb_title": tmdb_title,
                        "best_match": raw_title, "score": score,
                        "tmdb_year": tmdb_year, "mb_type": cand_type,
                    })

                if DEBUG_MODE and best_n:
                    self.logger.info(f"[DEBUG] {tmdb_title} ({tmdb_year}) best {TOP_N}:")
                    for cand, cand_score, _ in best_n:
                        self.logger.info(f"    {cand[:40]!r} → {cand_score}")

                bar.update(1)

        # Save results
        pd.DataFrame(matches).to_csv(self.output_matches, index=False)
        pd.DataFrame(misses).to_csv(self.output_unmatched, index=False)

        if GOLDEN_TEST_MODE:
            pd.DataFrame(golden_rows).to_csv(self.output_golden, index=False)
            fidelity = (golden_matched / golden_total * 100) if golden_total else 0
            coverage = (golden_with_candidates / golden_total * 100) if golden_total else 0
            self.logger.info(f"⭐ Golden Test: {golden_matched}/{golden_total} ({fidelity:.1f}%) | Coverage {coverage:.1f}%")

            STEP_METRICS.update({
                "golden_matched": golden_matched,
                "golden_total": golden_total,
                "golden_fidelity": fidelity,
                "golden_coverage": coverage,
            })

        self.logger.info(f"✅ Saved {len(matches)} matches, {len(misses)} unmatched → {self.output_matches.name}")

if __name__ == "__main__":
    step = Step08MatchTMDb()
    step.run()
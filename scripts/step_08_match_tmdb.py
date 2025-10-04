"""Step 08: Match TMDb
Attempts fuzzy matching between MusicBrainz soundtracks and TMDb titles.
Writes tmdb_match_results.csv, tmdb_match_unmatched.csv, and (in Golden Test Mode) tmdb_match_golden.csv.
"""

from base_step import BaseStep
import pandas as pd, requests, re
from rapidfuzz import fuzz, process
from config import (
    DEBUG_MODE, TMDB_DIR, TMDB_API_KEY, YEAR_VARIANCE,
    ROW_LIMIT, GOLDEN_TITLES, GOLDEN_TEST_MODE, STEP_METRICS
)
from utils import normalize_for_matching_extended
from tqdm import tqdm

# Debug toggles
TOP_N = 5  # Number of candidates to log in debug mode

# Centralized weights/bonuses/penalties
FUZZ_TOKEN_SET_WEIGHT = 0.7
FUZZ_PARTIAL_WEIGHT = 0.3
YEAR_PENALTY_PER_YEAR = 2
YEAR_PENALTY_MAX_YEARS = 3
OST_BONUS = 40
SUBSTRING_BONUS = 10
JUNK_PENALTY = -20
JUNK_TERMS = {"tribute", "karaoke", "cover", "best of", "inspired by", "volume", "vol."}

# Common stopwords to ignore when checking token overlap
STOPWORDS = {"the", "a", "an", "of", "in", "on", "and"}


def fetch_alt_titles(tmdb_id: str):
    """Fetch alternative titles from TMDb for fallback matching."""
    try:
        url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/alternative_titles"
        r = requests.get(url, params={"api_key": TMDB_API_KEY})
        r.raise_for_status()
        return [t["title"] for t in r.json().get("titles", []) if t.get("title")]
    except Exception:
        return []


def has_token_overlap(q: str, c: str) -> bool:
    """Require at least one meaningful token overlap between query and candidate."""
    q_tokens = {tok for tok in re.split(r"[^a-z0-9]+", q.lower()) if tok and tok not in STOPWORDS}
    c_tokens = {tok for tok in re.split(r"[^a-z0-9]+", c.lower()) if tok and tok not in STOPWORDS}
    return bool(q_tokens & c_tokens)


def composite_scorer(q, c, g_year=None, c_year=None, c_type=None):
    """Composite fuzzy scorer with year penalty, OST bonus, substring bonus, junk filter, and token overlap."""
    # Drop junk or non-overlapping candidates early
    if not has_token_overlap(q, c):
        return 0
    if any(term in c for term in JUNK_TERMS):
        return max(0, JUNK_PENALTY)

    s1 = fuzz.token_set_ratio(q, c)
    s2 = fuzz.partial_ratio(q, c)
    score = int(FUZZ_TOKEN_SET_WEIGHT * s1 + FUZZ_PARTIAL_WEIGHT * s2)

    # Year penalty
    if g_year and c_year:
        try:
            dy = abs(int(g_year) - int(c_year))
            score -= min(dy, YEAR_PENALTY_MAX_YEARS) * YEAR_PENALTY_PER_YEAR
        except Exception:
            pass

    # OST type bonus
    if isinstance(c_type, str) and c_type.lower() == "soundtrack":
        score += OST_BONUS

    # Substring bonus
    if q in c or c in q:
        score += SUBSTRING_BONUS

    return score


class Step08MatchTMDb(BaseStep):
    def __init__(self, name="Step 08: Match TMDb Titles", threshold=70.0):
        super().__init__(name)
        self.threshold = threshold

        # Input: curated TMDb movie list (Step 06 output)
        self.input_movies = TMDB_DIR / "enriched_top_1000.csv"

        # Input: cleaned MusicBrainz soundtrack candidates (Step 07 output)
        self.input_candidates = TMDB_DIR / "tmdb_input_candidates_clean.csv"

        # Outputs
        self.output_matches = TMDB_DIR / "tmdb_match_results.csv"
        self.output_unmatched = TMDB_DIR / "tmdb_match_unmatched.csv"
        self.output_golden = TMDB_DIR / "tmdb_match_golden.csv"

    def run(self):
        # 1) Load TMDb movies
        movies_df = pd.read_csv(self.input_movies, dtype={"tmdb_id": str})
        movies_df["normalized_title"] = movies_df["title"].apply(normalize_for_matching_extended)

        # Golden Test filter or ROW_LIMIT
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
        cands_df["normalized_title"] = cands_df["title"].apply(normalize_for_matching_extended)

        required = {"release_group_id", "title", "year", "normalized_title", "release_group_secondary_type"}
        if not required.issubset(set(cands_df.columns)):
            self.fail(f"Candidates missing required columns: {cands_df.columns.tolist()}")

        all_norms = cands_df["normalized_title"].tolist()
        norm_to_raw = {
            row.normalized_title: (row.title, row.release_group_id, row.year, getattr(row, "release_group_secondary_type", None))
            for row in cands_df.itertuples(index=False)
        }

        matches, misses, golden_rows = [], [], []

        # Golden test tracking
        golden_total = len([t for t in GOLDEN_TITLES if t in movies_df["title"].values])
        golden_matched = 0
        golden_with_candidates = 0

        # 3) Matching loop with tqdm
        with tqdm(total=len(movies_df), desc="Matching TMDb") as bar:
            for mv in movies_df.itertuples(index=False):
                tmdb_id, tmdb_title = mv.tmdb_id, mv.title
                tmdb_year = getattr(mv, "release_year", None)
                base_norm = mv.normalized_title

                # Candidate pool filtered by year, with simple fallback
                if tmdb_year:
                    cand_years = pd.to_numeric(cands_df["year"], errors="coerce")
                    mask = cand_years.between(
                        int(tmdb_year) - YEAR_VARIANCE,
                        int(tmdb_year) + YEAR_VARIANCE
                    )
                    pool_norms = cands_df.loc[mask, "normalized_title"].tolist()
                else:
                    # fallback: same first letter and length window
                    pool_norms = [n for n in all_norms if n and n[0] == base_norm[0] and abs(len(n.split()) - len(base_norm.split())) <= 3]

                if pool_norms:
                    golden_with_candidates += 1

                # Top-N fuzzy candidates
                best_n = process.extract(
                    base_norm,
                    pool_norms,
                    scorer=lambda q, c, **kwargs: composite_scorer(
                        q, c, tmdb_year, norm_to_raw.get(c, (None, None, None, None))[2], norm_to_raw.get(c, (None, None, None, None))[3]
                    ),
                    limit=TOP_N
                )

                if best_n:
                    best_norm, score, _ = best_n[0]
                else:
                    best_norm, score = None, -1

                # Alt titles fallback
                if score < self.threshold:
                    for alt in [normalize_for_matching_extended(t) for t in fetch_alt_titles(tmdb_id)]:
                        alt_best_n = process.extract(
                            alt,
                            pool_norms,
                            scorer=lambda q, c, **kwargs: composite_scorer(
                                q, c, tmdb_year, norm_to_raw.get(c, (None, None, None, None))[2], norm_to_raw.get(c, (None, None, None, None))[3]
                            ),
                            limit=TOP_N,
                        )
                        if alt_best_n and alt_best_n[0][1] > score:
                            best_norm, score, _ = alt_best_n[0]

                if best_norm:
                    raw_title, rgid, cand_year, cand_type = norm_to_raw.get(best_norm, (None, None, None, None))
                else:
                    raw_title, rgid, cand_year, cand_type = None, None, None, None

                # Year tolerance for Golden Set
                pass_year_check = True
                if tmdb_year and cand_year:
                    try:
                        tmdb_y, cand_y = int(tmdb_year), int(cand_year)
                        if tmdb_title in GOLDEN_TITLES:
                            if abs(tmdb_y - cand_y) > 2:
                                pass_year_check = False
                        else:
                            if tmdb_y != cand_y:
                                pass_year_check = False
                    except Exception:
                        pass_year_check = True

                # Save match vs miss
                if score >= self.threshold and rgid and pass_year_check:
                    row = {
                        "tmdb_id": tmdb_id,
                        "tmdb_title": tmdb_title,
                        "release_group_id": rgid,
                        "matched_title": raw_title,
                        "score": score,
                        "mb_year": cand_year,
                        "tmdb_year": tmdb_year,
                        "mb_type": cand_type,
                    }
                    matches.append(row)
                    golden_rows.append(row)
                    if tmdb_title in GOLDEN_TITLES:
                        golden_matched += 1
                else:
                    row = {
                        "tmdb_id": tmdb_id,
                        "tmdb_title": tmdb_title,
                        "best_match": raw_title,
                        "score": score,
                        "tmdb_year": tmdb_year,
                        "mb_type": cand_type,
                    }
                    misses.append(row)
                    golden_rows.append(row)

                # Debug log
                if DEBUG_MODE and best_n:
                    self.logger.info(f"[DEBUG] {tmdb_title} ({tmdb_year}) → top {TOP_N}:")
                    for cand, cand_score, _ in best_n:
                        raw, rgid, cand_year, cand_type = norm_to_raw.get(cand, ("?", "?", "?", "?"))
                        self.logger.info(f"    norm={cand!r}, raw={raw[:40]!r}, score={cand_score}, year={cand_year}, type={cand_type}")

                bar.update(1)

        # 4) Save results
        pd.DataFrame(matches).to_csv(self.output_matches, index=False)
        pd.DataFrame(misses).to_csv(self.output_unmatched, index=False)
        if GOLDEN_TEST_MODE:
            pd.DataFrame(golden_rows).to_csv(self.output_golden, index=False)

        self.logger.info(f"✅ Saved {len(matches)} matches, {len(misses)} unmatched")

        if GOLDEN_TEST_MODE:
            fidelity = (golden_matched / golden_total * 100) if golden_total else 0
            coverage = (golden_with_candidates / golden_total * 100) if golden_total else 0
            self.logger.info(
                f"⭐ Golden Test: {golden_matched}/{golden_total} matched ({fidelity:.1f}%) | Coverage: {coverage:.1f}%"
            )
            self.logger.info(f"⭐ Golden test results saved to {self.output_golden}")

            # Save to global metrics
            STEP_METRICS["golden_matched"] = golden_matched
            STEP_METRICS["golden_total"] = golden_total
            STEP_METRICS["golden_fidelity"] = fidelity
            STEP_METRICS["golden_coverage"] = coverage

            # Threshold sweep (debug only)
            if DEBUG_MODE:
                sweep_thresholds = range(65, 86, 2)
                self.logger.info("🔎 Golden Set Threshold Sweep:")
                for t in sweep_thresholds:
                    matched = sum(1 for row in golden_rows if row.get("score", -1) >= t and row.get("release_group_id"))
                    total = STEP_METRICS.get("golden_total", 0)
                    fidelity = (matched / total * 100) if total else 0
                    self.logger.info(f"  threshold={t} → {matched}/{golden_total} ({fidelity:.1f}%)")

        # 5) Write fresh documentation
        doc_text = f"""
Step 08 Output Documentation
============================

Generated by Step 08: Match TMDb Titles

Files Produced:
---------------
1. tmdb_match_results.csv → Successful matches (TMDb ↔ MB OST).
2. tmdb_match_unmatched.csv → Failed matches below threshold.
3. tmdb_match_golden.csv → Golden Test results (if GOLDEN_TEST_MODE=True).

Inputs:
-------
- enriched_top_1000.csv → TMDb movie list (Step 06).
- tmdb_input_candidates_clean.csv → MB OST pool (Step 05).

Run Context:
------------
- Threshold: {self.threshold}
- Golden Test Mode: {GOLDEN_TEST_MODE}
- Debug Mode: {DEBUG_MODE}, Top-N={TOP_N}
- ROW_LIMIT: {ROW_LIMIT or "∞"}
- Year Variance: ±{YEAR_VARIANCE}
"""
        doc_path = TMDB_DIR / "Step08_CSV_Documentation.txt"
        with open(doc_path, "w", encoding="utf-8") as f:
            f.write(doc_text)

        self.logger.info(f"📝 Wrote fresh documentation to {doc_path}")

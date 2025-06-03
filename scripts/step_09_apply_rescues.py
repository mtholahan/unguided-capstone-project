# step_09_apply_rescues.py

from base_step import BaseStep
import pandas as pd
from rapidfuzz import fuzz
from config import TMDB_DIR

class Step09ApplyRescues(BaseStep):
    def __init__(
        self,
        name: str = "Step 09: Apply Manual Rescues",
        threshold: float = 90.0,
        boost_per_genre: float = 5.0,
        vector_threshold: float = 0.6,
        enable_vector_matching: bool = False,
    ):
        super().__init__(name)
        self.threshold = threshold
        self.boost_per_genre = boost_per_genre
        self.vector_threshold = vector_threshold
        self.enable_vector_matching = enable_vector_matching

        # Input/Output files (all under TMDB_DIR)
        self.match_file        = TMDB_DIR / "tmdb_match_results.csv"
        self.manual_rescue     = TMDB_DIR / "manual_rescue.csv"
        self.enriched_movies   = TMDB_DIR / "enriched_top_1000.csv"
        self.candidates_file   = TMDB_DIR / "tmdb_input_candidates_clean.csv"
        self.output_enhanced   = TMDB_DIR / "tmdb_match_results_enhanced.csv"
        self.review_output     = TMDB_DIR / "top50_review.csv"

        # If vector matching is enabled, attempt to load SBERT
        if self.enable_vector_matching:
            try:
                from sentence_transformers import SentenceTransformer, util
                self.model = SentenceTransformer("all-MiniLM-L6-v2")
                self.util  = util
                self.enable_vector_matching = True
                self.logger.info("🔄 Loaded Sentence-BERT model for vector matching")
            except ImportError:
                self.logger.info("ℹ️ SBERT not installed → disabling vector matching")
                self.enable_vector_matching = False

    def run(self):
        # 1) Load base matches (from Step08)
        self.logger.info("🎬 Loading fuzzy‐match results for enhancement…")
        df = pd.read_csv(self.match_file, dtype=str)
        # Rename 'score' → 'match_score' if necessary
        if "score" in df.columns:
            df.rename(columns={"score": "match_score"}, inplace=True)

        # 2) Conditional genre boost (if requested)
        if self.boost_per_genre > 0:
            try:
                movies = pd.read_csv(self.enriched_movies, dtype=str)
                cands  = pd.read_csv(self.candidates_file, dtype=str)

                # Detect columns for genre‐list
                tmdb_id_col     = "tmdb_id"   if "tmdb_id" in movies.columns else "id" if "id" in movies.columns else None
                movie_genres_col= "genres"    if "genres" in movies.columns  else None
                rg_id_col       = "release_group_id" if "release_group_id" in cands.columns else None
                cand_genres_col = "genres"    if "genres" in cands.columns  else None

                if tmdb_id_col and rg_id_col and movie_genres_col and cand_genres_col:
                    # Prepare TMDb genre lists
                    movies = movies[[tmdb_id_col, movie_genres_col]].copy()
                    movies["tmdb_genres"] = movies[movie_genres_col].fillna("").str.split("|")
                    movies.rename(columns={tmdb_id_col: "tmdb_id"}, inplace=True)

                    # Prepare MB candidate genre lists
                    cands = cands[[rg_id_col, cand_genres_col]].copy()
                    cands["mb_genres"] = cands[cand_genres_col].fillna("").str.split("|")
                    cands.rename(columns={rg_id_col: "release_group_id"}, inplace=True)

                    # Merge genre info onto df
                    df = df.merge(
                        movies[["tmdb_id", "tmdb_genres"]],
                        on="tmdb_id", how="left"
                    ).merge(
                        cands[["release_group_id", "mb_genres"]],
                        on="release_group_id", how="left"
                    )

                    # Apply boost: add boost_per_genre for each overlapping genre
                    def apply_boost(row):
                        overlap = set(row.get("tmdb_genres", [])) & set(row.get("mb_genres", []))
                        return min(float(row["match_score"]) + len(overlap) * self.boost_per_genre, 100.0)

                    df["match_score"] = df.apply(apply_boost, axis=1)
                    self.logger.info(f"🎨 Applied genre boost: +{self.boost_per_genre} per genre overlap")
                else:
                    self.logger.info("🎨 Skipping genre boost: missing required columns")
            except Exception as e:
                self.logger.warning(f"⚠️ Error during genre boost: {e}")

        # 3) Produce a “Top 50” review file
        self.review_top_matches(df)

        # 4) Apply manual rescue entries
        df = self.apply_manual_rescue(df)

        # 5) Optional vector‐based filtering (SBERT)
        if self.enable_vector_matching:
            df["vector_score"] = df.apply(
                lambda r: float(
                    self.util.pytorch_cos_sim(
                        self.model.encode(r["tmdb_title"], convert_to_tensor=True),
                        self.model.encode(r["matched_title"], convert_to_tensor=True),
                    )[0][0]
                ), axis=1
            )
            before = len(df)
            df = df[(df["match_score"].astype(float) >= self.threshold) | (df["vector_score"] >= self.vector_threshold)]
            self.logger.info(f"🧠 Vector filtering: kept {len(df)} of {before} by threshold {self.vector_threshold}")

        # 6) Final threshold filter and write
        final_df = df[df["match_score"].astype(float) >= self.threshold]
        final_df.to_csv(self.output_enhanced, index=False)
        self.logger.info(f"💾 Enhanced matches saved to {self.output_enhanced.name} ({len(final_df)} rows)")

    def review_top_matches(self, df: pd.DataFrame, top_n: int = 50):
        """
        Take the top N rows by match_score and flag any low‐score or short‐title rows.
        Writes to `self.review_output`.
        """
        def flag(row):
            if float(row["match_score"]) < self.threshold:
                return "LOW_SCORE"
            if len(str(row.get("tmdb_title", ""))) < 5 or len(str(row.get("matched_title", ""))) < 5:
                return "SHORT_TITLE"
            return ""

        top50 = df.sort_values(by="match_score", ascending=False).head(top_n).copy()
        top50["flag"] = top50.apply(flag, axis=1)
        top50.to_csv(self.review_output, index=False)
        self.logger.info(f"🔎 Top {top_n} matches reviewed → {self.review_output.name}")

    def apply_manual_rescue(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Load `manual_rescue.csv`, compute fuzzy rescue scores on (tmdb_title vs. mb_title),
        and append any new rescue‐valid rows to df.
        """
        try:
            rescue = pd.read_csv(self.manual_rescue, dtype=str)
        except FileNotFoundError:
            self.logger.info("🪫 No manual_rescue.csv found; skipping rescue")
            return df
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load manual_rescue.csv: {e}")
            return df

        # Ensure rescue has both tmdb_title and mb_title
        if "tmdb_title" not in rescue.columns or "mb_title" not in rescue.columns:
            # Attempt to map mb_title via candidates_file
            try:
                cands = pd.read_csv(self.candidates_file, dtype=str)
                if "title" in cands.columns and "release_group_id" in cands.columns:
                    map_titles = cands.set_index("release_group_id")["title"].to_dict()
                    rescue["mb_title"] = rescue["release_group_id"].map(map_titles)
                else:
                    raise KeyError("Candidates file missing 'title' or 'release_group_id'")
            except Exception as e:
                self.logger.warning(f"Skipping manual rescue: cannot map mb_title ({e})")
                return df

        # Compute rescue_score
        try:
            rescue["rescue_score"] = rescue.apply(
                lambda r: fuzz.token_set_ratio(str(r["tmdb_title"]), str(r["mb_title"])), axis=1
            )
        except KeyError as e:
            self.logger.warning(f"Skipping manual rescue scoring: missing column {e}")
            return df

        # Filter valid rescues and append any not already in df
        valid = rescue[rescue["rescue_score"].astype(float) >= self.threshold]
        existing_ids = set(df["tmdb_id"].astype(str))
        additions = []
        for _, r in valid.iterrows():
            if str(r["tmdb_id"]) not in existing_ids:
                additions.append({
                    "tmdb_id":        r["tmdb_id"],
                    "tmdb_title":     r["tmdb_title"],
                    "matched_title":  r["mb_title"],
                    "match_score":    r["rescue_score"],
                    "release_group_id": r["release_group_id"]
                })
        if additions:
            df = pd.concat([df, pd.DataFrame(additions)], ignore_index=True)
            self.logger.info(f"🛟 Added {len(additions)} manual rescue rows")
        else:
            self.logger.info("ℹ️ No new manual rescues to add")

        return df

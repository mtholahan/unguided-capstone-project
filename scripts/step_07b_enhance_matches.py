import pandas as pd
from rapidfuzz import fuzz
from base_step import BaseStep
from config import DATA_DIR

try:
    from sentence_transformers import SentenceTransformer, util
    HAS_SBERT = True
except ImportError:
    HAS_SBERT = False


class Step07bEnhanceMatches(BaseStep):
    def __init__(
        self,
        name: str = "Step 07b: Enhance Matches",
        threshold: float = 90.0,
        boost_per_genre: float = 5.0,
        vector_threshold: float = 0.6,
        enable_vector_matching: bool = False,
    ):
        super().__init__(name)
        self.threshold = threshold
        self.boost_per_genre = boost_per_genre
        self.vector_threshold = vector_threshold
        self.enable_vector_matching = enable_vector_matching and HAS_SBERT
        self.match_file = DATA_DIR / "tmdb" / "tmdb_match_results.csv"
        self.manual_rescue = DATA_DIR / "tmdb" / "manual_rescue.csv"
        self.enriched_movies = DATA_DIR / "tmdb" / "enriched_top_1000.csv"
        self.candidates_file = DATA_DIR / "tmdb" / "tmdb_input_candidates.csv"
        self.output_enhanced = DATA_DIR / "tmdb" / "tmdb_match_results_enhanced.csv"
        self.review_output = DATA_DIR / "tmdb" / "top50_review.csv"
        if self.enable_vector_matching:
            self.logger.info("🔄 Loading Sentence-BERT model...")
            self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def run(self):
        # Load and standardize matches
        df = pd.read_csv(self.match_file)
        if 'score' in df.columns:
            df.rename(columns={'score': 'match_score'}, inplace=True)

        # Conditional genre boost
        if self.boost_per_genre > 0:
            try:
                movies = pd.read_csv(self.enriched_movies)
                cands = pd.read_csv(self.candidates_file)
                # Determine ID column
                tmdb_id_col = 'id' if 'id' in movies.columns else 'tmdb_id' if 'tmdb_id' in movies.columns else None
                rg_id_col = 'release_group_id' if 'release_group_id' in cands.columns else None
                # Determine genre columns
                movie_genre_col = 'genres' if 'genres' in movies.columns else None
                cand_genre_col = 'genres' if 'genres' in cands.columns else None

                if tmdb_id_col and rg_id_col and movie_genre_col and cand_genre_col:
                    movies = movies[[tmdb_id_col, movie_genre_col]].copy()
                    cands = cands[[rg_id_col, cand_genre_col]].copy()
                    movies['tmdb_genres'] = movies[movie_genre_col].fillna('').apply(lambda x: x.split('|'))
                    cands['mb_genres'] = cands[cand_genre_col].fillna('').apply(lambda x: x.split('|'))
                    df = df.merge(
                        movies.rename(columns={tmdb_id_col: 'tmdb_id', 'tmdb_genres': 'tmdb_genres'}),
                        on='tmdb_id', how='left'
                    ).merge(
                        cands.rename(columns={rg_id_col: 'release_group_id', 'mb_genres': 'mb_genres'}),
                        on='release_group_id', how='left'
                    )

                    def apply_boost(row):
                        overlap = set(row.get('tmdb_genres', [])) & set(row.get('mb_genres', []))
                        return min(row['match_score'] + len(overlap) * self.boost_per_genre, 100.0)

                    df['match_score'] = df.apply(apply_boost, axis=1)
                    self.logger.info(f"🎨 Applied genre boost: +{self.boost_per_genre} per genre overlap")
                else:
                    self.logger.info("🎨 Skipping genre boost: required columns not found")
            except Exception as e:
                self.logger.warning(f"Error during genre boost step: {e}")

        # Review top matches
        self.review_top_matches(df)

        # Apply manual rescue
        df = self.apply_manual_rescue(df)

        # Vector-based rescoring & filtering
        if self.enable_vector_matching:
            df['vector_score'] = df.apply(
                lambda row: float(
                    util.pytorch_cos_sim(
                        self.model.encode(row['tmdb_title'], convert_to_tensor=True),
                        self.model.encode(row['matched_title'], convert_to_tensor=True)
                    )[0][0]
                ), axis=1
            )
            pre_filter_count = len(df)
            df = df[(df['match_score'] >= self.threshold) | (df['vector_score'] >= self.vector_threshold)]
            self.logger.info(
                f"🧠 Vector filtering: kept {len(df)} of {pre_filter_count} by threshold {self.vector_threshold}"
            )

        # Final threshold filter and save
        final = df[df['match_score'] >= self.threshold]
        final.to_csv(self.output_enhanced, index=False)
        self.logger.info(f"💾 Final enhanced matches saved to {self.output_enhanced.name}")

    def review_top_matches(self, df: pd.DataFrame, top_n: int = 50):
        def flag(row):
            if row['match_score'] < self.threshold:
                return 'LOW_SCORE'
            if len(str(row.get('tmdb_title', ''))) < 5 or len(str(row.get('matched_title', ''))) < 5:
                return 'SHORT_TITLE'
            return ''

        top = df.sort_values(by='match_score', ascending=False).head(top_n).copy()
        top['flag'] = top.apply(flag, axis=1)
        top.to_csv(self.review_output, index=False)
        self.logger.info(f"🔎 Top {top_n} matches reviewed and saved to {self.review_output.name}")

        def apply_manual_rescue(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            rescue = pd.read_csv(self.manual_rescue)
        except Exception as e:
            self.logger.warning(f"⚠️ Could not load manual_rescue.csv: {e}")
            return df

        # Identify tmdb and mb title columns
        if 'tmdb_title' not in rescue.columns or 'mb_title' not in rescue.columns:
            try:
                # Try to map mb_title from candidates file
                cands = pd.read_csv(self.candidates_file)
                if 'title' in cands.columns:
                    map_titles = cands.set_index('release_group_id')['title'].to_dict()
                    rescue['mb_title'] = rescue['release_group_id'].map(map_titles)
                else:
                    raise KeyError('title')
            except Exception as e:
                self.logger.warning(f"Skipping manual rescue: cannot map mb_title ({e})")
                return df

        # Compute rescue scores with error handling
        try:
            rescue['rescue_score'] = rescue.apply(
                lambda r: fuzz.token_set_ratio(str(r['tmdb_title']), str(r['mb_title'])), axis=1
            )
        except KeyError as e:
            self.logger.warning(f"Skipping manual rescue scoring: missing column {e}")
            return df

        valid = rescue[rescue['rescue_score'] >= self.threshold]
        existing_ids = set(df['tmdb_id'].astype(str))
        additions = []
        for _, r in valid.iterrows():
            if str(r['tmdb_id']) not in existing_ids:
                additions.append({
                    'tmdb_id': r['tmdb_id'],
                    'tmdb_title': r.get('tmdb_title', ''),
                    'matched_title': r.get('mb_title', ''),
                    'match_score': r['rescue_score']
                })
        if additions:
            df = pd.concat([df, pd.DataFrame(additions)], ignore_index=True)
            self.logger.info(f"🛟 Added {len(additions)} manual rescues")
        else:
            self.logger.info(f"ℹ️ No new manual rescues added")
        return df

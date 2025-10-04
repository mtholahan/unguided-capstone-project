"""Step 07: Prepare TMDb Input
Prepares TMDb dataset for matching by normalizing fields (title, year).
Writes tmdb_input_candidates_clean.csv to TMDB_DIR.
"""

from base_step import BaseStep
import pandas as pd
from config import DATA_DIR, TMDB_DIR, DEBUG_MODE
from utils import normalize_title_for_matching, is_mostly_digits
from tqdm import tqdm

class Step07PrepareTMDbInput(BaseStep):
    def __init__(self, name="Step 07: Prepare TMDb Input"):
        super().__init__(name)
        self.input_tsv  = DATA_DIR / "soundtracks.tsv"
        self.output_csv = TMDB_DIR / "tmdb_input_candidates_clean.csv"

    def run(self):
        self.logger.info("🔍 Loading soundtracks with headers...")
        df = pd.read_csv(self.input_tsv, sep="\t", header=0, dtype=str, engine="python")

        # Ensure release_group_secondary_type exists (optional fallback)
        if "release_group_secondary_type" not in df.columns:
            df["release_group_secondary_type"] = None

        # Confirm required columns exist (after adding fallback)
        required_cols = {"release_group_id", "release_year", "raw_row", "release_group_secondary_type"}
        if not required_cols.issubset(df.columns):
            self.fail(f"Missing required columns in {self.input_tsv}: {required_cols - set(df.columns)}")

        # Hardened title extraction
        def extract_title(raw):
            try:
                parts = raw.split("|")
                if len(parts) > 2 and parts[2].strip():
                    return parts[2].strip()
                if len(parts) > 1 and parts[1].strip():
                    return parts[1].strip()
                if parts and parts[0].strip():
                    return parts[0].strip()
                return None
            except Exception:
                return None

        self.logger.info("🔧 Extracting titles...")
        df["title"] = [extract_title(raw) for raw in tqdm(df["raw_row"], desc="Extracting titles")]

        # Normalize titles
        self.logger.info("🔧 Normalizing titles...")
        df["normalized_title"] = [normalize_title_for_matching(t) for t in tqdm(df["title"], desc="Normalizing")]

        # Filter out invalid rows
        initial_count = len(df)
        self.logger.info(f"ℹ️ Initial row count: {initial_count}")

        def is_valid(row):
            nt = row["normalized_title"]
            if not nt or len(nt) < 3:
                return False
            if is_mostly_digits(nt):
                return False
            try:
                yr = int(row["release_year"])
            except:
                return False
            return (1900 <= yr <= 2025)

        filtered = df[df.apply(is_valid, axis=1)]
        removed = initial_count - len(filtered)
        self.logger.info(f"🧹 Removed {removed} invalid or out-of-range titles")

        # Hybrid OST keyword + type filter
        OST_TERMS = {"soundtrack", "ost", "score", "motion picture"}
        DENY_TERMS = {"tribute", "karaoke", "remix", "mix", "volume", "vol.", "greatest hits", "best of"}

        def passes_hybrid_filter(row) -> bool:
            title = row["title"] or ""
            t = title.lower()

            if any(term in t for term in DENY_TERMS):
                return False

            # Keep if explicitly tagged as soundtrack AND has OST-like terms
            if str(row.get("release_group_secondary_type", "")).lower() == "soundtrack":
                if any(term in t for term in OST_TERMS):
                    return True
                # Special case: allow bare movie titles if type is Soundtrack
                return True if len(t.split()) <= 5 else False

            return False

        filtered = filtered[filtered.apply(passes_hybrid_filter, axis=1)]

        # Drop duplicates on (normalized_title, release_year)
        self.logger.info("🧼 Dropping duplicates...")
        filtered = filtered.drop_duplicates(subset=["normalized_title", "release_year"])

        # Final output with clean schema (always includes release_group_secondary_type)
        out_df = pd.DataFrame({
            "normalized_title":  filtered["normalized_title"],
            "title":             filtered["title"],
            "release_group_id":  filtered["release_group_id"],
            "year":              filtered["release_year"].astype(int),
            "release_group_secondary_type": filtered["release_group_secondary_type"]
        })

        # Debug preview
        if DEBUG_MODE:
            self.logger.info("🔎 Debug Preview (first 10 rows):")
            preview = out_df.head(10).to_dict(orient="records")
            for row in preview:
                self.logger.info(
                    f"   rgid={row['release_group_id']}, "
                    f"title={row['title']}, "
                    f"norm={row['normalized_title']}, "
                    f"year={row['year']}, "
                    f"type={row['release_group_secondary_type']}"
                )

        final_count = len(out_df)
        out_df.to_csv(self.output_csv, index=False)
        self.logger.info(f"✅ Final output row count: {final_count}")
        self.logger.info(f"✅ Saved to {self.output_csv}")

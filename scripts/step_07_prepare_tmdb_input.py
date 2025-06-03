import pandas as pd
import re
from base_step import BaseStep
from config import DATA_DIR
from utils import normalize_title_for_matching, is_mostly_digits

class Step07PrepareTMDbInput(BaseStep):
    def __init__(self, name="Step 07: Prepare TMDb Input"):
        super().__init__(name)
        self.input_tsv   = DATA_DIR / "soundtracks.tsv"
        self.junk_list   = DATA_DIR / "junk_mb_titles.txt"
        self.output_csv  = DATA_DIR / "tmdb" / "tmdb_input_candidates_clean.csv"

    def run(self):
        self.logger.info("🔍 Loading soundtracks...")
        df = pd.read_csv(self.input_tsv, sep="\t", dtype=str, header=0, engine='python')

        # If 'title' missing, use heuristic: pick column with alphabetic strings
        title_col = None
        if "title" in df.columns:
            title_col = "title"
        else:
            for col in df.columns:
                sample = str(df[col].iat[0])
                if re.search(r"[A-Za-z]", sample) and not re.fullmatch(r"\d+", sample):
                    title_col = col
                    break
        if not title_col:
            self.logger.error(f"No heuristic 'title' column found. Columns: {df.columns.tolist()}")
            raise KeyError("Cannot find a title column in soundtracks.tsv")
        self.logger.info(f"ℹ️ Using '{title_col}' as the title column")

        # Find 'release_group_id' or UUID-like column
        rg_col = None
        uuid_re = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
        for col in df.columns:
            if col.lower().startswith("release_group") or df[col].astype(str).str.match(uuid_re).any():
                rg_col = col
                break
        if not rg_col:
            self.logger.error(f"No 'release_group_id' column found. Columns: {df.columns.tolist()}")
            raise KeyError("Cannot find a release_group_id column in soundtracks.tsv")
        self.logger.info(f"ℹ️ Using '{rg_col}' as the release_group_id column")

        # Find 'release_year' column: look for four-digit year
        year_col = None
        for col in df.columns:
            if df[col].astype(str).str.match(r"^\d{4}$").any():
                year_col = col
                break
        if not year_col:
            self.logger.error(f"No 'release_year' column found. Columns: {df.columns.tolist()}")
            raise KeyError("Cannot find a release_year column in soundtracks.tsv")
        self.logger.info(f"ℹ️ Using '{year_col}' as the release_year column")

        # Normalize titles
        self.logger.info("🔧 Normalizing titles...")
        df["normalized_title"] = df[title_col].apply(normalize_title_for_matching)

        # Filter: drop titles <3 chars, junk, mostly numeric, out–of–range year
        initial_count = len(df)
        self.logger.info(f"ℹ️ Initial row count: {initial_count}")

        junk = set()
        if self.junk_list.exists():
            with open(self.junk_list) as f:
                junk = {line.strip().lower() for line in f if line.strip()}
            self.logger.info(f"ℹ️ Loaded {len(junk)} junk titles to filter")
        else:
            self.logger.info("ℹ️ No junk_mb_titles.txt found; skipping junk‐title filtering")

        def is_valid(row):
            nt = row["normalized_title"]
            if len(nt) < 3:
                return False
            if nt in junk:
                return False
            if is_mostly_digits(nt):
                return False
            yr_val = str(row[year_col])
            try:
                yr = int(yr_val)
            except:
                return False
            return (1900 <= yr <= 2025)

        df = df[df.apply(is_valid, axis=1)]
        removed = initial_count - len(df)
        self.logger.info(f"🧹 Removed {removed} invalid or out-of-range titles")

        # Deduplicate on (normalized_title, release_year)
        self.logger.info("🧼 Dropping duplicates...")
        df = df.drop_duplicates(subset=["normalized_title", year_col])

        # Assemble output DataFrame with the four required columns
        out_df = pd.DataFrame({
            "normalized_title": df["normalized_title"],
            "title":            df[title_col],
            "release_group_id": df[rg_col],
            "year":             df[year_col].astype(int)
        })

        final_count = len(out_df)
        out_df.to_csv(self.output_csv, index=False)
        self.logger.info(f"✅ Final output row count: {final_count}")
        self.logger.info(f"✅ Saved to {self.output_csv}")
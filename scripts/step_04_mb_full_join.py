"""Step 04: MusicBrainz Full Join
Joins release, release_group, and artist_credit into a consolidated dataset.
Writes joined_release_data.tsv to DATA_DIR.
"""

from base_step import BaseStep
import csv
from config import MB_CLEANSED_DIR, DATA_DIR, TSV_WHITELIST, ROW_LIMIT
from tqdm import tqdm


class Step04MBFullJoin(BaseStep):
    def __init__(self, name="Step 04: MB Full Join"):
        super().__init__(name)
        self.data = {}

    def load_file(self, name):
        """Load a MusicBrainz TSV file into memory (streamed if possible)."""
        csv.field_size_limit(1_000_000)
        path = MB_CLEANSED_DIR / name
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            rows = [row for row in reader]
        return rows

    def run(self):
        # Ensure required files are available
        required_files = ["release", "release_group", "artist_credit"]
        for fname in required_files:
            if fname not in TSV_WHITELIST:
                self.fail(f"Required file {fname} missing from whitelist.")
                return

        # --- Load smaller lookup tables ---
        self.data["release_group"] = self.load_file("release_group")
        self.logger.info(f"Loaded release_group: {len(self.data['release_group']):,} rows")

        self.data["artist_credit"] = self.load_file("artist_credit")
        self.logger.info(f"Loaded artist_credit: {len(self.data['artist_credit']):,} rows")

        # --- Optional pre-filter for Soundtrack release groups ---
        header = self.data["release_group"][0]
        header_lower = [h.lower() for h in header]
        if "secondary_type" in header_lower:
            idx = header_lower.index("secondary_type")
            pre_count = len(self.data["release_group"])
            self.data["release_group"] = [
                row for row in self.data["release_group"]
                if len(row) > idx and "soundtrack" in row[idx].lower()
            ]
            post_count = len(self.data["release_group"])
            self.logger.info(
                f"🎚️  Filtered release_group by 'Soundtrack' type: {pre_count:,} → {post_count:,}"
            )
        else:
            self.logger.warning("⚠️  No 'secondary_type' column found; skipping soundtrack filter.")

        # --- Build lookup dictionaries ---
        release_group_map = {row[0]: row for row in self.data["release_group"]}
        artist_credit_map = {row[0]: row for row in self.data["artist_credit"]}

        # --- Prepare output ---
        output_path = DATA_DIR / "joined_release_data.tsv"
        release_path = MB_CLEANSED_DIR / "release"
        row_count = sum(1 for _ in open(release_path, encoding="utf-8"))
        effective_limit = ROW_LIMIT or row_count

        self.logger.info(f"🔄 Streaming {row_count:,} releases for join (ROW_LIMIT={effective_limit:,})")

        joined_rows = 0
        with open(release_path, encoding="utf-8") as fin, \
             open(output_path, "w", encoding="utf-8", newline="") as fout:

            reader = csv.reader(fin, delimiter="\t")
            writer = csv.writer(fout, delimiter="\t")

            with tqdm(total=min(row_count, effective_limit), desc="Joining Releases") as bar:
                for i, row in enumerate(reader, start=1):
                    if ROW_LIMIT and i > ROW_LIMIT:
                        break
                    if len(row) < 4:
                        continue

                    release_group_id = row[1]  # FK → release_group
                    artist_credit_id = row[3]  # FK → artist_credit
                    rg = release_group_map.get(release_group_id, [])
                    ac = artist_credit_map.get(artist_credit_id, [])
                    writer.writerow(row + rg + ac)

                    joined_rows += 1
                    if joined_rows % 100_000 == 0:
                        self.logger.info(f"Processed {joined_rows:,} joined rows...")

                    bar.update(1)

        self.logger.info(
            f"✅ [DONE] Joined {joined_rows:,} rows → {output_path.name} "
            f"(limit={ROW_LIMIT or '∞'})"
        )

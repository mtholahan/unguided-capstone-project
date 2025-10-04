"""Step 05: Filter Soundtracks
Filters the joined dataset to include only soundtrack releases.
Cross-references release_group_secondary_type and extracts release years.
Writes soundtracks.tsv to DATA_DIR, with guaranteed release_group_id column.
"""

from base_step import BaseStep
import csv, re
from config import DATA_DIR, MB_RAW_DIR, ROW_LIMIT
from tqdm import tqdm


class Step05FilterSoundtracks(BaseStep):
    def __init__(self, name="Step 05: Filter Soundtracks"):
        super().__init__(name)

    def load_secondary_type_map(self):
        """Return set of release_group_ids tagged as 'soundtrack'."""
        path = MB_RAW_DIR / "release_group_secondary_type_join"
        if not path.exists():
            self.fail(f"Missing file: {path}")
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            if len(header) < 2:
                self.fail(f"Unexpected header in {path}: {header}")
            soundtrack_ids = {row[0] for row in reader if row[1] == "1"}
        self.logger.info(f"Loaded {len(soundtrack_ids):,} soundtrack IDs")
        return soundtrack_ids

    def load_release_year_map(self):
        """
        Build map release_group_id → earliest release year
        using fixed column positions from MusicBrainz release.tsv.
        """
        path = MB_RAW_DIR / "release"
        if not path.exists():
            self.fail(f"Missing file: {path}")

        year_map = {}
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                if len(row) < 13:
                    continue
                rgid = row[4].strip()
                date_str = row[-1].strip()
                if not rgid or not date_str or date_str == "\\N":
                    continue
                m = re.match(r"(\d{4})", date_str)
                if not m:
                    continue
                year = int(m.group(1))
                prev = year_map.get(rgid)
                if prev is None or year < prev:
                    year_map[rgid] = year

        self.logger.info(f"Built release_year_map with {len(year_map):,} entries")
        return year_map

    def run(self):
        joined_path = DATA_DIR / "joined_release_data.tsv"
        output_path = DATA_DIR / "soundtracks.tsv"

        if not joined_path.exists():
            self.fail(f"Missing input file: {joined_path}")

        soundtrack_ids = self.load_secondary_type_map()
        release_year_map = self.load_release_year_map()

        matched, skipped = 0, 0

        # Count rows for progress bar
        row_count = sum(1 for _ in open(joined_path, encoding="utf-8"))
        self.logger.info(f"🔄 Scanning {row_count:,} joined releases for soundtracks (ROW_LIMIT={ROW_LIMIT or '∞'})")

        with open(joined_path, encoding="utf-8") as fin, \
             open(output_path, "w", encoding="utf-8", newline="") as fout:

            reader = csv.reader(fin, delimiter='\t')
            writer = csv.writer(fout, delimiter='\t')

            # Force schema: always include release_group_id, release_year, raw_row, and release_group_secondary_type
            out_header = ["release_group_id", "release_year", "raw_row", "release_group_secondary_type"]
            writer.writerow(out_header)

            with tqdm(total=min(row_count, ROW_LIMIT or row_count), desc="Filtering Soundtracks") as bar:
                for i, row in enumerate(reader, start=1):
                    if ROW_LIMIT and i > ROW_LIMIT:
                        break

                    if len(row) < 5:
                        skipped += 1
                        bar.update(1)
                        continue

                    release_group_id = row[4]  # col 4 = release_group_id
                    if release_group_id not in soundtrack_ids:
                        skipped += 1
                    else:
                        year = release_year_map.get(release_group_id, -1)
                        # Mark as Soundtrack explicitly
                        writer.writerow([release_group_id, year, "|".join(row), "Soundtrack"])
                        matched += 1
                    bar.update(1)

        self.logger.info(f"[DONE] Wrote {matched:,} soundtrack rows → {output_path} ({skipped:,} skipped)")

        # ✅ Post-validation
        with open(output_path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            required = {"release_group_id", "release_year", "raw_row", "release_group_secondary_type"}
            if not required.issubset(header):
                self.fail("Output schema missing required columns")
            else:
                self.logger.info("✅ Output schema validated: contains release_group_id, release_year, release_group_secondary_type")
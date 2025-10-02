# step_05_filter_soundtracks.py

from base_step import BaseStep
import csv
from config import DATA_DIR, MB_RAW_DIR
import re

class Step05FilterSoundtracks(BaseStep):
    def __init__(self, name="Step 05: Filter Soundtracks"):
        super().__init__(name)

    def load_secondary_type_map(self):
        path = MB_RAW_DIR / "release_group_secondary_type_join"
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            soundtrack_ids = {row[0] for row in reader if row[1] == "1"}
        return soundtrack_ids

    def load_release_year_map(self):
        """
        Build a map from release_group_id → earliest release_year (int),
        by scanning MB_RAW_DIR / "release" (tab-delimited). 
        """

        path = MB_RAW_DIR / "release"
        year_map = {}

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)
            # Find column indexes for release_group and date
            try:
                rg_index = header.index("release_group")
                date_index = header.index("date")
            except ValueError:
                self.logger.error("Could not find 'release_group' or 'date' column in release.tsv header")
                return year_map

            for row in reader:
                rgid = row[rg_index].strip()
                date_str = row[date_index].strip()  # e.g. "1994-09-14"
                if not date_str or date_str == "\\N":
                    continue
                m = re.match(r"(\d{4})", date_str)
                if not m:
                    continue
                year = int(m.group(1))
                prev = year_map.get(rgid)
                if prev is None or year < prev:
                    year_map[rgid] = year

        self.logger.info(f"Loaded release_year_map with {len(year_map):,} entries from release.tsv")
        return year_map

    def run(self):
        joined_path = DATA_DIR / "joined_release_data.tsv"
        output_path = DATA_DIR / "soundtracks.tsv"

        if not joined_path.exists():
            self.fail(f"Missing input file: {joined_path}")

        # 1) Load the set of release_group_ids that are soundtracks
        soundtrack_ids = self.load_secondary_type_map()

        # 2) Build a map from release_group_id → release_year
        release_year_map = self.load_release_year_map()

        matched = []
        skipped = 0

        with open(joined_path, encoding="utf-8") as fin:
            reader = csv.reader(fin, delimiter='\t')
            for row in self.progress_iter(reader, desc="Filtering Soundtracks"):
                # NOTE: row[4] is the release_group_id in joined_release_data.tsv
                release_group_id = row[4]
                if release_group_id not in soundtrack_ids:
                    skipped += 1
                    continue

                # Lookup the real release year (or None if missing)
                year = release_year_map.get(release_group_id)
                if year is None:
                    # If you want to drop any OSTs that have no valid year, you could:
                    #    continue
                    # Or you can assign a placeholder (e.g., 0 or -1):
                    year = -1

                # OPTIONAL DEBUG: see that blockbusters are included
                title = row[2]  # OST title column in joined_release_data.tsv
                for term in ["Shawshank", "Godfather", "Pulp Fiction", "Forrest Gump"]:
                    if term.lower() in title.lower():
                        self.logger.debug(f"Adding OST candidate → Title: {title!r}, Year: {year}, RGID: {release_group_id}")

                # Append the original row PLUS the new year column at the end
                matched.append(row + [str(year)])

        # Write out soundtracks.tsv with the extra column for year
        with open(output_path, "w", encoding="utf-8", newline="") as fout:
            writer = csv.writer(fout, delimiter='\t')
            writer.writerows(matched)

        self.logger.info(f"[DONE] Wrote {len(matched):,} soundtracks to {output_path} ({skipped:,} skipped)")

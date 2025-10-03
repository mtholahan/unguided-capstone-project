"""Step 05: Filter Soundtracks
Filters the joined dataset to include only soundtrack releases.
Cross-references release_group_secondary_type and extracts release years.
Writes soundtracks.tsv to DATA_DIR.
"""

from base_step import BaseStep
import csv, re
from pathlib import Path
from config import DATA_DIR, MB_RAW_DIR


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
        Schema: 
        0=id, 1=gid, 2=name, 3=artist_credit, 
        4=release_group, 5=status, 6=packaging,
        7=language, 8=script, 9=barcode, 
        10=comment, 11=edit, 12=last_updated, 
        13=date_added
        But in dumps, 'date' may actually be at index 12 or 13.
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
                # Pick date from last column that looks like YYYY-MM-DD
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

        matched, skipped = [], 0

        with open(joined_path, encoding="utf-8") as fin:
            reader = csv.reader(fin, delimiter='\t')
            header = next(reader)
            # Append new "year" column
            out_header = header + ["release_year"]

            for row in self.progress_iter(reader, desc="Filtering Soundtracks"):
                release_group_id = row[4]  # column 4 is release_group_id
                if release_group_id not in soundtrack_ids:
                    skipped += 1
                    continue
                year = release_year_map.get(release_group_id, -1)
                matched.append(row + [str(year)])

        with open(output_path, "w", encoding="utf-8", newline="") as fout:
            writer = csv.writer(fout, delimiter='\t')
            writer.writerow(out_header)
            writer.writerows(matched)

        self.logger.info(f"[DONE] Wrote {len(matched):,} soundtracks to {output_path} ({skipped:,} skipped)")

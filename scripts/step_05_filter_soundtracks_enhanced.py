"""Step 05 Enhanced: Filter Soundtracks (+ Analytics and Parquet Output)
-----------------------------------------------------------------------
Filters the joined dataset to include only soundtrack releases.
Adds release-year extraction, writes both TSV and Parquet versions,
and logs Power BI–compatible metrics.

Input :  DATA_DIR/joined_release_data.tsv
Outputs: DATA_DIR/soundtracks.tsv
         DATA_DIR/soundtracks.parquet
         DATA_DIR/soundtracks_subset.parquet (optional)
"""

from base_step import BaseStep
from config import DATA_DIR, MB_RAW_DIR, ROW_LIMIT
import csv, re
from tqdm import tqdm
import pandas as pd


class Step05FilterSoundtracksEnhanced(BaseStep):
    def __init__(self, name="Step 05 Enhanced: Filter Soundtracks"):
        super().__init__(name)

    # -------------------------------------------------------------
    def load_secondary_type_map(self):
        """Return set of release_group_ids tagged as 'soundtrack'."""
        path = MB_RAW_DIR / "release_group_secondary_type_join"
        if not path.exists():
            self.fail(f"Missing file: {path}")

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            header = next(reader, [])
            if len(header) < 2:
                self.fail(f"Unexpected header in {path}: {header}")
            soundtrack_ids = {row[0] for row in reader if len(row) >= 2 and row[1] == "1"}

        self.logger.info(f"🎵 Loaded {len(soundtrack_ids):,} soundtrack IDs from secondary_type_join")
        return soundtrack_ids

    # -------------------------------------------------------------
    def load_release_year_map(self):
        """Map release_group_id → earliest release year (from release.tsv)."""
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

        self.logger.info(f"📅 Built release_year_map with {len(year_map):,} entries")
        return year_map

    # -------------------------------------------------------------
    def run(self):
        joined_path = DATA_DIR / "joined_release_data.tsv"
        output_tsv = DATA_DIR / "soundtracks.tsv"
        output_parquet = DATA_DIR / "soundtracks.parquet"

        if not joined_path.exists():
            self.fail(f"Missing input file: {joined_path}")

        soundtrack_ids = self.load_secondary_type_map()
        release_year_map = self.load_release_year_map()

        matched, skipped = 0, 0
        row_count = sum(1 for _ in open(joined_path, encoding="utf-8"))
        effective_limit = ROW_LIMIT or row_count

        self.logger.info(
            f"🔍 Scanning {row_count:,} joined releases for soundtracks "
            f"(ROW_LIMIT = {effective_limit:,})"
        )

        with open(joined_path, encoding="utf-8") as fin, \
             open(output_tsv, "w", encoding="utf-8", newline="") as fout:

            reader = csv.reader(fin, delimiter="\t")
            writer = csv.writer(fout, delimiter="\t")

            out_header = ["release_group_id", "release_year", "raw_row", "release_group_secondary_type"]
            writer.writerow(out_header)

            with tqdm(total=min(row_count, effective_limit), desc="Filtering Soundtracks") as bar:
                for i, row in enumerate(reader, start=1):
                    if ROW_LIMIT and i > ROW_LIMIT:
                        break
                    if not row or len(row) < 5:
                        skipped += 1
                        bar.update(1)
                        continue

                    # Find release_group_id safely
                    release_group_id = None
                    for cell in row:
                        if cell.isdigit() and len(cell) >= 5:
                            release_group_id = cell
                            break
                    release_group_id = release_group_id or row[4]

                    if release_group_id not in soundtrack_ids:
                        skipped += 1
                    else:
                        year = release_year_map.get(release_group_id, -1)
                        writer.writerow([release_group_id, year, "|".join(row), "Soundtrack"])
                        matched += 1

                    if i % 100_000 == 0:
                        self.logger.info(f"Processed {i:,} rows... (matched = {matched:,})")
                    bar.update(1)

        self.logger.info(f"✅ Wrote {matched:,} soundtrack rows → {output_tsv.name} ({skipped:,} skipped)")

        # ✅ Post-validation
        with open(output_tsv, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            header = next(reader)
            required = {"release_group_id", "release_year", "raw_row", "release_group_secondary_type"}
            if not required.issubset(header):
                self.fail("Output schema missing required columns")
            else:
                self.logger.info("✅ Output schema validated correctly.")

        # 📦 Write Parquet copy for downstream steps
        df = pd.read_csv(output_tsv, sep="\t")
        df.to_parquet(output_parquet, index=False)
        self.logger.info(f"📦 Saved Parquet version → {output_parquet.name} ({len(df):,} rows)")

        # 🎯 Optional subset for validation
        subset_fraction = getattr(self.config, "SAMPLE_FRACTION", 0.02)
        if 0 < subset_fraction < 1.0:
            sample = df.sample(frac=subset_fraction, random_state=42)
            sample_path = DATA_DIR / "soundtracks_subset.parquet"
            sample.to_parquet(sample_path, index=False)
            self.logger.info(
                f"🎯 Saved subset ({subset_fraction*100:.1f}% = {len(sample):,} rows) → {sample_path.name}"
            )

        # 📊 Metrics for Power BI tracking
        metrics = {
            "rows_total": row_count,
            "rows_matched": matched,
            "rows_skipped": skipped,
            "match_pct": round(100 * matched / max(row_count, 1), 2),
        }
        self.write_metrics("step05_filter_soundtracks", metrics)
        self.logger.info(f"📈 Metrics logged → Power BI ({metrics})")


if __name__ == "__main__":
    step = Step05FilterSoundtracksEnhanced()
    step.run()

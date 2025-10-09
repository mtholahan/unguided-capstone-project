# v2.1 — Year inference & metrics
"""Step 05 Enhanced: Filter Soundtracks (+ Year Repair, Analytics, Parquet)
--------------------------------------------------------------------------
Filters the joined dataset to include only soundtrack releases.
Repairs missing/invalid release years from raw text, writes TSV/Parquet,
and logs Power BI–compatible metrics.

Input :  DATA_DIR/joined_release_data.tsv
Lookups: MB_CLEANSED_DIR/release_group_secondary_type_join.tsv
         MB_CLEANSED_DIR/release.tsv
Outputs: DATA_DIR/soundtracks.tsv
         DATA_DIR/soundtracks.parquet
         DATA_DIR/soundtracks_subset.parquet (optional)
"""

from base_step import BaseStep
from config import DATA_DIR, MB_CLEANSED_DIR, ROW_LIMIT
import csv, re
import pandas as pd
from pathlib import Path
from datetime import datetime

CURRENT_YEAR = datetime.now().year

class Step05FilterSoundtracksEnhanced(BaseStep):
    def __init__(self, name="Step 05 Enhanced: Filter Soundtracks"):
        super().__init__(name=name)

    # -------------------------------------------------------------
    def resolve_file(self, basename: str):
        """Return the first matching file (.tsv or extensionless) under MB_CLEANSED_DIR."""
        candidates = [
            MB_CLEANSED_DIR / f"{basename}.tsv",
            MB_CLEANSED_DIR / basename,
        ]
        for p in candidates:
            if p.exists():
                return p
        self.fail(f"Missing file: {basename}(.tsv) not found in {MB_CLEANSED_DIR}")

    # -------------------------------------------------------------
    def load_secondary_type_map(self):
        """Return set of release_group_ids tagged as 'soundtrack'."""
        path = self.resolve_file("release_group_secondary_type_join")
        self.logger.info(f"🎼 Using secondary-type map from {path}")

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            header = next(reader, [])
            if len(header) < 2:
                self.fail(f"Unexpected header in {path}: {header}")
            soundtrack_ids = {row[0] for row in reader if len(row) >= 2 and row[1] == "1"}

        self.logger.info(f"🎵 Loaded {len(soundtrack_ids):,} soundtrack IDs from cleansed dataset")
        return soundtrack_ids

    # -------------------------------------------------------------
    def load_release_year_map(self):
        """Map release_group_id → earliest release year (from cleansed release.tsv)."""
        path = self.resolve_file("release")
        self.logger.info(f"📅 Loading release_year_map from {path}")

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

        self.logger.info(f"📅 Built release_year_map with {len(year_map):,} entries from cleansed data")
        return year_map

    # -------------------------------------------------------------
    @staticmethod
    def infer_year_from_text(text: str) -> int:
        """Infer a plausible year from free text (e.g., raw_row). Returns -1 if unknown."""
        if not text:
            return -1
        # prefer 4-digit 19xx/20xx; take earliest occurrence
        m = re.search(r"(19|20)\d{2}", text)
        if not m:
            return -1
        year = int(m.group(0))
        return year if 1900 <= year <= (CURRENT_YEAR + 1) else -1

    # -------------------------------------------------------------
    def run(self):
        self.setup_logger()
        self.logger.info("🚀 Starting Step 05: Filter Soundtracks (Enhanced + Year Repair)")

        joined_path = DATA_DIR / "joined_release_data.tsv"
        output_tsv = DATA_DIR / "soundtracks.tsv"
        output_parquet = DATA_DIR / "soundtracks.parquet"

        if not joined_path.exists():
            self.fail(f"Missing input file: {joined_path}")
            return

        soundtrack_ids = self.load_secondary_type_map()
        release_year_map = self.load_release_year_map()

        matched, skipped = 0, 0
        repaired_count, empty_year_count = 0, 0
        row_count = sum(1 for _ in open(joined_path, encoding="utf-8"))
        effective_limit = ROW_LIMIT or row_count
        self.logger.info(
            f"🔍 Scanning {row_count:,} joined releases for soundtracks (ROW_LIMIT = {effective_limit:,})"
        )

        # --- Filter Soundtracks ---
        with open(joined_path, encoding="utf-8") as fin, \
             open(output_tsv, "w", encoding="utf-8", newline="") as fout:

            reader = csv.reader(fin, delimiter="\t")
            writer = csv.writer(fout, delimiter="\t")
            out_header = ["release_group_id", "release_year", "raw_row", "release_group_secondary_type"]
            writer.writerow(out_header)

            for _ in self.progress_iter(range(effective_limit), desc="Filtering Soundtracks"):
                try:
                    row = next(reader)
                except StopIteration:
                    break

                if not row or len(row) < 5:
                    skipped += 1
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
                    raw_text = "|".join(row)
                    year = release_year_map.get(release_group_id, -1)
                    if year is None:
                        year = -1
                    # repair year if missing/invalid
                    if not (1900 <= int(year) <= (CURRENT_YEAR + 1)):
                        empty_year_count += 1
                        inferred = self.infer_year_from_text(raw_text)
                        if inferred != -1:
                            year = inferred
                            repaired_count += 1
                        else:
                            year = -1
                    writer.writerow([release_group_id, year, raw_text, "Soundtrack"])
                    matched += 1

                if (matched + skipped) % 100_000 == 0:
                    self.logger.info(f"📈 Processed {matched + skipped:,} rows... (matched = {matched:,})")

        self.logger.info(f"💾 Wrote {matched:,} soundtrack rows → {output_tsv.name} ({skipped:,} skipped)")
        self.logger.info(f"🩹 Year repair: empty/invalid={empty_year_count:,}, repaired={repaired_count:,}")

        # --- Post-validation ---
        with open(output_tsv, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            header = next(reader)
            required = {"release_group_id", "release_year", "raw_row", "release_group_secondary_type"}
            if not required.issubset(header):
                self.fail("Output schema missing required columns")
                return
        self.logger.info("✅ Output schema validated correctly.")

        # --- Parquet Export ---
        df = pd.read_csv(output_tsv, sep="\t")
        df.to_parquet(output_parquet, index=False)
        self.logger.info(f"📦 Saved Parquet version → {output_parquet.name} ({len(df):,} rows)")

        # --- Optional Subset ---
        subset_fraction = getattr(self.config, "SAMPLE_FRACTION", 0.02)
        if 0 < subset_fraction < 1.0:
            sample = df.sample(frac=subset_fraction, random_state=42)
            sample_path = DATA_DIR / "soundtracks_subset.parquet"
            sample.to_parquet(sample_path, index=False)
            self.logger.info(
                f"🎯 Saved subset ({subset_fraction*100:.1f}% = {len(sample):,} rows) → {sample_path.name}"
            )

        # --- Metrics ---
        metrics = {
            "rows_total": row_count,
            "rows_matched": matched,
            "rows_skipped": skipped,
            "match_pct": round(100 * matched / max(row_count, 1), 2),
            "row_limit_active": bool(ROW_LIMIT),
            "source_dir": str(MB_CLEANSED_DIR),
            "years_empty_or_invalid": empty_year_count,
            "years_repaired": repaired_count,
        }
        self.write_metrics("step05_filter_soundtracks", metrics)
        self.logger.info(f"📊 Metrics recorded: {metrics}")
        self.logger.info("✅ [DONE] Step 05 completed successfully (enhanced).")


if __name__ == "__main__":
    Step05FilterSoundtracksEnhanced().run()
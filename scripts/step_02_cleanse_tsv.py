"""Step 02: Cleanse TSV
Cleanses raw TSV files by removing nulls, malformed rows, or oversized fields.
Respects global ROW_LIMIT to physically cap file sizes.
Outputs cleansed copies back to MB_CLEANSED_DIR.
"""

from base_step import BaseStep
import csv
from config import MB_RAW_DIR, MB_CLEANSED_DIR, TSV_WHITELIST, ROW_LIMIT
from tqdm import tqdm

class Step02CleanseTSV(BaseStep):
    def __init__(self, name="Step 02: Cleanse TSV"):
        super().__init__(name)

    def run(self):
        MB_CLEANSED_DIR.mkdir(parents=True, exist_ok=True)
        raw_files = sorted([
            f for f in MB_RAW_DIR.iterdir()
            if f.name in TSV_WHITELIST
        ])

        if not raw_files:
            self.logger.warning("⚠️  No files found to clean.")
            return

        csv.field_size_limit(1_000_000)
        self.logger.info(f"🧹 Cleansing {len(raw_files)} TSV files from {MB_RAW_DIR} (ROW_LIMIT={ROW_LIMIT or '∞'})")

        for infile in self.progress_iter(raw_files, desc="Cleansing TSVs"):
            outfile = MB_CLEANSED_DIR / infile.name
            try:
                with open(infile, mode="r", encoding="utf-8", errors="replace") as fin, \
                     open(outfile, mode="w", encoding="utf-8", newline="") as fout:

                    reader = csv.reader(fin, delimiter="\t")
                    writer = csv.writer(fout, delimiter="\t")

                    headers = next(reader, [])
                    writer.writerow([h.strip() for h in headers])

                    row_count = 0
                    skipped = 0
                    total_rows = sum(1 for _ in open(infile, encoding="utf-8", errors="replace"))
                    effective_limit = ROW_LIMIT or total_rows

                    with tqdm(total=min(total_rows, effective_limit), desc=infile.name[:30], leave=False) as bar:
                        for i, row in enumerate(reader, start=1):
                            if ROW_LIMIT and i > ROW_LIMIT:
                                self.logger.info(f"[ROW_LIMIT] Stopping early after {ROW_LIMIT:,} rows ({infile.name})")
                                break
                            if len(row) != len(headers):
                                skipped += 1
                                continue
                            writer.writerow([cell.strip() for cell in row])
                            row_count += 1
                            if i % 100_000 == 0:
                                self.logger.info(f"{infile.name}: processed {i:,} rows")
                            bar.update(1)

                self.logger.info(
                    f"✅ Cleaned: {infile.name} → {outfile.name} "
                    f"({row_count:,} rows, {skipped:,} skipped)"
                )

            except Exception as e:
                self.logger.error(f"❌ Error processing {infile.name}: {e}")

        self.logger.info(f"[DONE] Cleansed {len(raw_files)} TSV files → {MB_CLEANSED_DIR}")

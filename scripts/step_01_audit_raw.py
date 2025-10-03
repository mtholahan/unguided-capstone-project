"""Step 01: Audit Raw
Scans whitelisted TSVs for row counts, column counts, and basic integrity.
Respects ROW_LIMIT if set in config.py.
"""

from base_step import BaseStep
import csv
from config import MB_RAW_DIR, TSV_WHITELIST, ROW_LIMIT


class Step01AuditRaw(BaseStep):
    def __init__(self, name="Step 01 Audit Raw"):
        super().__init__(name)

    def run(self):
        raw_files = sorted([
            f for f in MB_RAW_DIR.iterdir()
            if f.name in TSV_WHITELIST
        ])

        if not raw_files:
            self.logger.warning(f"No whitelisted files found in {MB_RAW_DIR}")
            return

        self.logger.info(f"Auditing {len(raw_files)} files in: {MB_RAW_DIR}")

        csv.field_size_limit(1_000_000)
        for tsv_path in self.progress_iter(raw_files, desc="Auditing TSVs"):
            try:
                with open(tsv_path, encoding="utf-8") as f:
                    reader = csv.reader(f, delimiter='\t')
                    headers = next(reader)

                    row_count = 0
                    for row in reader:
                        row_count += 1
                        if ROW_LIMIT and row_count >= ROW_LIMIT:
                            self.logger.info(
                                f"[ROW_LIMIT] Stopping early after {ROW_LIMIT:,} rows"
                            )
                            break

                self.logger.info(
                    f"{tsv_path.name}: {row_count:,} rows (limited by ROW_LIMIT={ROW_LIMIT})"
                    if ROW_LIMIT else
                    f"{tsv_path.name}: {row_count:,} rows, {len(headers)} columns"
                )

            except Exception as e:
                self.logger.error(f"Error reading {tsv_path.name}: {e}")

        self.logger.info(f"[DONE] Audited {len(raw_files)} TSV files.")

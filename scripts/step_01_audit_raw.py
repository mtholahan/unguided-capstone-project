"""Step 01: Audit Raw
Scans whitelisted TSVs for row counts, column counts, and basic integrity.
Respects ROW_LIMIT if set in config.py and provides progress feedback.
"""

from base_step import BaseStep
import csv
from config import MB_RAW_DIR, TSV_WHITELIST, ROW_LIMIT
from tqdm import tqdm


class Step01AuditRaw(BaseStep):
    def __init__(self, name="Step 01: Audit Raw"):
        super().__init__(name)

    def run(self):
        raw_files = sorted([
            f for f in MB_RAW_DIR.iterdir()
            if f.name in TSV_WHITELIST
        ])

        if not raw_files:
            self.logger.warning(f"⚠️  No whitelisted files found in {MB_RAW_DIR}")
            return

        self.logger.info(f"🧾 Auditing {len(raw_files)} raw TSV files from {MB_RAW_DIR} (ROW_LIMIT={ROW_LIMIT or '∞'})")

        csv.field_size_limit(1_000_000)

        for tsv_path in self.progress_iter(raw_files, desc="Auditing TSVs"):
            try:
                with open(tsv_path, encoding="utf-8", errors="replace") as f:
                    reader = csv.reader(f, delimiter="\t")
                    try:
                        headers = next(reader)
                    except StopIteration:
                        self.logger.warning(f"{tsv_path.name}: empty file — skipping.")
                        continue

                    total_rows = sum(1 for _ in open(tsv_path, encoding="utf-8", errors="replace")) - 1
                    effective_limit = ROW_LIMIT or total_rows
                    row_count = 0

                    with tqdm(total=min(total_rows, effective_limit), desc=tsv_path.name[:30], leave=False) as bar:
                        for i, row in enumerate(reader, start=1):
                            if ROW_LIMIT and i > ROW_LIMIT:
                                self.logger.info(f"[ROW_LIMIT] Stopping early after {ROW_LIMIT:,} rows ({tsv_path.name})")
                                break
                            row_count += 1
                            bar.update(1)

                msg = (
                    f"{tsv_path.name}: {row_count:,} rows (limited by ROW_LIMIT={ROW_LIMIT})"
                    if ROW_LIMIT
                    else f"{tsv_path.name}: {row_count:,} rows, {len(headers)} columns"
                )
                self.logger.info(msg)

            except Exception as e:
                self.logger.error(f"❌ Error reading {tsv_path.name}: {e}")

        self.logger.info(f"[DONE] Audited {len(raw_files)} TSV files → {MB_RAW_DIR}")

"""Step 03: Check Structure
Verifies that cleansed TSVs have the expected column structure.
Respects ROW_LIMIT to avoid reading entire large files during validation.
"""

from base_step import BaseStep
import csv
from collections import Counter
from config import MB_CLEANSED_DIR, TSV_WHITELIST, ROW_LIMIT
from tqdm import tqdm

class Step03CheckStructure(BaseStep):
    def __init__(self, name="Step 03: Check Structure"):
        super().__init__(name)

    def run(self):
        input_files = sorted([
            f for f in MB_CLEANSED_DIR.iterdir()
            if f.name in TSV_WHITELIST
        ])

        if not input_files:
            self.logger.warning("⚠️  No cleansed files found for structure check.")
            return

        csv.field_size_limit(1_000_000)
        self.logger.info(f"🔍 Checking structure of {len(input_files)} cleansed TSVs (ROW_LIMIT={ROW_LIMIT or '∞'})")

        for fpath in self.progress_iter(input_files, desc="Checking TSV Structure"):
            try:
                with open(fpath, encoding="utf-8") as f:
                    reader = csv.reader(f, delimiter="\t")
                    try:
                        header = next(reader)
                    except StopIteration:
                        self.logger.warning(f"{fpath.name}: Empty file — skipping.")
                        continue

                    header_len = len(header)
                    col_counts = Counter()

                    # Count rows up to ROW_LIMIT
                    total_rows = sum(1 for _ in open(fpath, encoding="utf-8")) - 1
                    effective_limit = ROW_LIMIT or total_rows

                    with tqdm(total=min(total_rows, effective_limit), desc=fpath.name[:30], leave=False) as bar:
                        for i, row in enumerate(reader, start=1):
                            if ROW_LIMIT and i > ROW_LIMIT:
                                self.logger.info(f"[ROW_LIMIT] Stopping early after {ROW_LIMIT:,} rows ({fpath.name})")
                                break
                            col_counts[len(row)] += 1
                            if i % 100_000 == 0:
                                self.logger.info(f"{fpath.name}: processed {i:,} rows")
                            bar.update(1)

                # Analyze mismatches
                issues = {k: v for k, v in col_counts.items() if k != header_len}
                if issues:
                    self.logger.warning(
                        f"{fpath.name}: Column mismatch! "
                        f"Expected {header_len}, Found: {issues}"
                    )
                else:
                    self.logger.info(f"{fpath.name}: Structure OK ({header_len} columns)")

            except Exception as e:
                self.logger.error(f"❌ Error processing {fpath.name}: {e}")

        self.logger.info(f"[DONE] Structure check complete for {len(input_files)} files.")

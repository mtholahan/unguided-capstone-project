"""Step 03: Check Structure
Verifies that cleansed TSVs have the expected column structure.
Respects ROW_LIMIT to avoid reading entire large files during validation.
"""

from base_step import BaseStep
import csv
from collections import Counter
from config import MB_CLEANSED_DIR, TSV_WHITELIST, ROW_LIMIT, DEBUG_MODE
from utils import make_progress_bar


class Step03CheckStructure(BaseStep):
    def __init__(self, name="Step 03: Check Structure"):
        super().__init__(name="Step 03: Check Structure")

    def run(self):
        # --- Flexible matching: accept files with or without .tsv extensions ---
        input_files = []
        for f in MB_CLEANSED_DIR.iterdir():
            base = f.stem  # name without extension
            if base in TSV_WHITELIST or f.name in TSV_WHITELIST:
                input_files.append(f)

        input_files = sorted(input_files)

        if not input_files:
            self.logger.warning(
                f"⚠️  No cleansed files found in {MB_CLEANSED_DIR}. "
                "Check TSV_WHITELIST and file extensions."
            )
            available = [p.name for p in MB_CLEANSED_DIR.iterdir()]
            self.logger.info(f"Files available in directory: {available}")
            return

        if not input_files:
            self.logger.warning("⚠️  No cleansed files found for structure check.")
            return

        csv.field_size_limit(1_000_000)
        self.logger.info(
            f"🔍 Checking structure of {len(input_files)} cleansed TSVs (ROW_LIMIT={ROW_LIMIT or '∞'})"
        )

        # ---- Main loop ----
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

                    total_rows = sum(1 for _ in open(fpath, encoding="utf-8")) - 1
                    effective_limit = ROW_LIMIT or total_rows
                    bar_desc = fpath.name[:30]

                    # ✅ Unified progress bar
                    with make_progress_bar(total=min(total_rows, effective_limit),
                                           desc=bar_desc,
                                           leave=False,
                                           unit="rows") as bar:
                        for i, row in enumerate(reader, start=1):
                            if ROW_LIMIT and i > ROW_LIMIT:
                                self.logger.info(
                                    f"[ROW_LIMIT] Stopping early after {ROW_LIMIT:,} rows ({fpath.name})"
                                )
                                break

                            col_counts[len(row)] += 1
                            if i % 100_000 == 0:
                                self.logger.info(f"{fpath.name}: processed {i:,} rows")
                            bar.update(1)

                # Analyze mismatches
                issues = {k: v for k, v in col_counts.items() if k != header_len}
                if issues:
                    self.logger.warning(
                        f"{fpath.name}: Column mismatch! Expected {header_len}, Found: {issues}"
                    )
                else:
                    self.logger.info(f"{fpath.name}: Structure OK ({header_len} columns)")

            except Exception as e:
                self.logger.error(f"❌ Error processing {fpath.name}: {e}")

        self.logger.info(f"[DONE] Structure check complete for {len(input_files)} files.")


if __name__ == "__main__":
    step = Step03CheckStructure()
    step.run()

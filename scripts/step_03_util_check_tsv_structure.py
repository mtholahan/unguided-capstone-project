"""Step 03: Check Structure
Verifies that cleansed TSVs have the expected column structure.
Respects ROW_LIMIT to avoid reading entire large files during validation.
"""

from base_step import BaseStep
import csv
from pathlib import Path
from collections import Counter
from config import MB_CLEANSED_DIR, TSV_WHITELIST, ROW_LIMIT, DEBUG_MODE


class Step03CheckStructure(BaseStep):
    def __init__(self, name="Step 03: Check Structure"):
        super().__init__(name=name)

    # ------------------------------------------------------------------
    def run(self):
        self.setup_logger()
        # --- Flexible matching: accept files with or without .tsv extensions ---
        whitelist_stems = {Path(f).stem for f in TSV_WHITELIST}
        input_files = sorted([
            f for f in MB_CLEANSED_DIR.iterdir()
            if f.stem in whitelist_stems or f.name in TSV_WHITELIST
        ])

        if not input_files:
            self.logger.warning(
                f"⚠️ No cleansed files found in {MB_CLEANSED_DIR}. "
                "Check TSV_WHITELIST and file extensions."
            )
            available = [p.name for p in MB_CLEANSED_DIR.iterdir()]
            self.logger.info(f"Files available in directory: {available}")
            return

        csv.field_size_limit(1_000_000)
        self.logger.info(
            f"🔍 Checking structure of {len(input_files)} cleansed TSVs (ROW_LIMIT={ROW_LIMIT or '∞'})"
        )

        metrics = {"files_checked": 0, "files_with_mismatch": 0}

        # ---- Main loop ----
        for fpath in self.progress_iter(input_files, desc="Checking TSV Structure", unit="file"):
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

                    for i, row in enumerate(
                        self.progress_iter(reader, desc=fpath.name[:30], unit="row", leave=False),
                        start=1,
                    ):
                        if ROW_LIMIT and i > ROW_LIMIT:
                            self.logger.info(
                                f"[ROW_LIMIT] Stopping early after {ROW_LIMIT:,} rows ({fpath.name})"
                            )
                            break
                        col_counts[len(row)] += 1

                # Analyze mismatches
                issues = {k: v for k, v in col_counts.items() if k != header_len}
                metrics["files_checked"] += 1
                if issues:
                    self.logger.warning(
                        f"{fpath.name}: Column mismatch! Expected {header_len}, Found: {issues}"
                    )
                    metrics["files_with_mismatch"] += 1
                else:
                    self.logger.info(f"{fpath.name}: Structure OK ({header_len} columns)")

            except Exception as e:
                self.logger.error(f"❌ Error processing {fpath.name}: {e}")

        # ------------------------------------------------------------------
        self.write_metrics("step03_check_tsv_structure", metrics)
        self.logger.info(f"📊 Metrics logged: {metrics}")
        self.logger.info(f"[DONE] Structure check complete for {metrics['files_checked']} files.")


if __name__ == "__main__":
    Step03CheckStructure().run()

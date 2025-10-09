"""Step 01: Audit Raw
Scans whitelisted TSVs for row counts, column counts, and basic integrity.
Respects ROW_LIMIT if set in config.py and provides progress feedback.
"""

from base_step import BaseStep
import csv
import pandas as pd
from pathlib import Path
from config import MB_RAW_DIR, TSV_WHITELIST, ROW_LIMIT, DEBUG_MODE, AUDIT_SAMPLE_LIMIT


class Step01AuditRaw(BaseStep):
    def __init__(self, name="Step 01: Audit Raw"):
        super().__init__(name=name)
        self.output_summary = MB_RAW_DIR / "audit_summary.csv"

    # ------------------------------------------------------------------
    def run(self):
        self.setup_logger()
        # Normalize whitelist to allow entries with or without ".tsv"
        tsv_whitelist = {
            Path(f).stem.lower()  # e.g. "artist" from "artist.tsv"
            for f in TSV_WHITELIST
        }

        raw_files = [
            f for f in MB_RAW_DIR.glob("*.tsv")
            if f.stem.lower() in tsv_whitelist
        ]

        # 🧠 Skip if audit summary already exists
        if self.output_summary.exists():
            self.logger.info(f"✅ Audit summary already exists at {self.output_summary}. Skipping Step 01.")
            return

        if not raw_files:
            self.logger.warning(f"⚠️ No whitelisted TSVs found in {MB_RAW_DIR}. Did Step 00 run?")
            return

        self.logger.info(
            f"🧾 Auditing {len(raw_files)} TSVs in {MB_RAW_DIR} "
            f"(ROW_LIMIT={ROW_LIMIT or '∞'})"
        )

        csv.field_size_limit(AUDIT_SAMPLE_LIMIT)
        results = []

        # ------------------------------------------------------------------
        for tsv_path in self.progress_iter(raw_files, desc="Auditing TSVs", unit="file"):
            row_count = 0
            col_count = 0
            error_flag = None

            try:
                with open(tsv_path, encoding="utf-8", errors="replace") as f:
                    reader = csv.reader(f, delimiter="\t")
                    header = next(reader, [])
                    col_count = len(header)

                    # Show the first 30 characters of the file name in the progress bar.
                    for row in self.progress_iter(reader, desc=tsv_path.name[:30], unit="row", leave=False):
                        row_count += 1
                        if ROW_LIMIT and row_count >= ROW_LIMIT:
                            self.logger.info(
                                f"[ROW_LIMIT] Stopping early after {ROW_LIMIT:,} rows ({tsv_path.name})"
                            )
                            break

            except Exception as e:
                error_flag = str(e)
                self.logger.warning(f"❌ Error reading {tsv_path.name}: {e}")

            results.append(
                {
                    "filename": tsv_path.name,
                    "rows": row_count,
                    "cols": col_count,
                    "error": error_flag or "",
                }
            )

        # ------------------------------------------------------------------
        df = pd.DataFrame(results)
        df.to_csv(self.output_summary, index=False, encoding="utf-8")
        self.logger.info(f"✅ Audit summary written to {self.output_summary}")

        if DEBUG_MODE:
            self.logger.info("🔎 Audit preview:")
            self.logger.info(df.head().to_string())

        metrics = {
            "files_audited": len(df),
            "files_with_errors": int(df["error"].astype(bool).sum()),
            "rows_total": int(df["rows"].sum()),
        }
        self.write_metrics("step01_audit_raw", metrics)
        self.logger.info(f"📊 Metrics logged: {metrics}")
        self.logger.info("✅ Step 01 complete.")


if __name__ == "__main__":
    Step01AuditRaw().run()

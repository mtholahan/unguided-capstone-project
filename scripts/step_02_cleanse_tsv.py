"""Step 02: Cleanse TSV
Cleanses raw TSV files by removing nulls, malformed rows, or oversized fields.
Respects global ROW_LIMIT to physically cap file sizes.
Outputs cleansed copies back to MB_CLEANSED_DIR and adds release_year enrichment.
"""

from base_step import BaseStep
import csv
import re
import pandas as pd
from pathlib import Path
from config import MB_RAW_DIR, MB_CLEANSED_DIR, TSV_WHITELIST, ROW_LIMIT, DEBUG_MODE


class Step02CleanseTSV(BaseStep):
    def __init__(self, name="Step 02: Cleanse TSV"):
        super().__init__(name=name)

    # ------------------------------------------------------------------
    def run(self):
        self.setup_logger()
        MB_CLEANSED_DIR.mkdir(parents=True, exist_ok=True)

        # Normalize whitelist to allow entries with or without ".tsv"
        whitelist_stems = {Path(f).stem for f in TSV_WHITELIST}
        raw_files = sorted(
            [f for f in MB_RAW_DIR.glob("*") if f.stem in whitelist_stems]
        )

        # 🧠 Skip if all cleansed TSVs already exist
        cleansed_files = [f for f in MB_CLEANSED_DIR.glob("*.tsv")]
        cleansed_stems = {f.stem for f in cleansed_files}
        missing = sorted(whitelist_stems - cleansed_stems)

        if not missing:
            self.logger.info(f"✅ All cleansed TSVs already exist in {MB_CLEANSED_DIR}. Skipping Step 02.")
            return

        if not raw_files:
            self.logger.warning("⚠️ No raw files found to clean. Did Step 00 run?")
            return

        csv.field_size_limit(1_000_000)
        self.logger.info(
            f"🧹 Cleansing {len(raw_files)} TSV files from {MB_RAW_DIR} "
            f"(ROW_LIMIT={ROW_LIMIT or '∞'})"
        )

        metrics = {"files_processed": 0, "rows_written": 0, "rows_skipped": 0}

        # ------------------------------------------------------------------
        for infile in self.progress_iter(raw_files, desc="Cleansing TSVs", unit="file"):
            name = infile.stem + ".tsv"
            outfile = MB_CLEANSED_DIR / name

            try:
                with open(infile, "r", encoding="utf-8", errors="replace") as fin, \
                     open(outfile, "w", encoding="utf-8", newline="") as fout:

                    reader = csv.reader(fin, delimiter="\t")
                    writer = csv.writer(fout, delimiter="\t")

                    headers = next(reader, [])
                    headers = [h.strip() for h in headers]
                    writer.writerow(headers)

                    row_count = 0
                    skipped = 0

                    for i, row in enumerate(
                        self.progress_iter(reader, desc=infile.name[:30], unit="rows", leave=False),
                        start=1,
                    ):
                        if ROW_LIMIT and i > ROW_LIMIT:
                            self.logger.info(
                                f"[ROW_LIMIT] Stopping after {ROW_LIMIT:,} rows ({infile.name})"
                            )
                            break

                        if len(row) != len(headers):
                            skipped += 1
                            continue

                        writer.writerow([cell.strip() for cell in row])
                        row_count += 1

                    self.logger.info(
                        f"✅ Cleaned: {infile.name} → {outfile.name} "
                        f"({row_count:,} rows, {skipped:,} skipped)"
                    )
                    metrics["files_processed"] += 1
                    metrics["rows_written"] += row_count
                    metrics["rows_skipped"] += skipped

            except Exception as e:
                self.logger.error(f"❌ Error processing {infile.name}: {e}")

        # ------------------------------------------------------------------
        self.logger.info(
            f"[DONE] Cleansed {metrics['files_processed']} TSV files → {MB_CLEANSED_DIR}"
        )

        # ------------------------------------------------------------------
        # Step 2b – Derive release_year
        try:
            possible_paths = [
                MB_CLEANSED_DIR / "release.tsv",
                MB_CLEANSED_DIR / "release",
            ]
            release_path = next((p for p in possible_paths if p.exists()), None)

            if not release_path:
                self.logger.warning("⚠️ release.tsv not found in cleansed directory; skipping enrichment.")
                self.write_metrics("step02_cleanse_tsv", metrics)
                return

            self.logger.info(f"Found release file: {release_path}")
            df = pd.read_csv(release_path, sep="\t", low_memory=False)

            # --- Find date/year columns automatically
            date_cols = [c for c in df.columns if re.search("date|year", c, re.I)]
            if not date_cols:
                sample = df.head(20)
                for c in df.columns:
                    if sample[c].astype(str).str.contains(r"\d{4}", regex=True).any():
                        date_cols.append(c)

            if not date_cols:
                self.logger.warning("⚠️ No date-like columns found; cannot derive release_year.")
                self.write_metrics("step02_cleanse_tsv", metrics)
                return

            cand = df[date_cols[0]].astype(str)
            df["release_year"] = cand.map(
                lambda v: int(m.group(1)) if (m := re.search(r"(\d{4})", v)) else pd.NA
            ).astype("Int64")

            out_path = MB_CLEANSED_DIR / "release_enriched.tsv"
            df.to_csv(out_path, sep="\t", index=False)

            coverage = df["release_year"].notna().mean()
            self.logger.info(f"✅ Added release_year → {out_path.name} (coverage={coverage:.1%})")
            metrics["release_year_coverage"] = round(coverage, 3)

            if DEBUG_MODE:
                self.logger.info("🔎 Preview enriched rows:")
                self.logger.info(df.head(3).to_string())

        except Exception as e:
            self.logger.error(f"❌ Error deriving release_year: {e}")

        # ------------------------------------------------------------------
        self.write_metrics("step02_cleanse_tsv", metrics)
        self.logger.info(f"📊 Metrics logged: {metrics}")
        self.logger.info("✅ Step 02 complete.")


if __name__ == "__main__":
    Step02CleanseTSV().run()

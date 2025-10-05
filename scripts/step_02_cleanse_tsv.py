"""Step 02: Cleanse TSV
Cleanses raw TSV files by removing nulls, malformed rows, or oversized fields.
Respects global ROW_LIMIT to physically cap file sizes.
Outputs cleansed copies back to MB_CLEANSED_DIR and adds release_year enrichment.
"""

from base_step import BaseStep
import csv
from config import MB_RAW_DIR, MB_CLEANSED_DIR, TSV_WHITELIST, ROW_LIMIT
from tqdm import tqdm
import logging
from pathlib import Path


class Step02CleanseTSV(BaseStep):
    def __init__(self, name="Step 02: Cleanse TSV"):
        super().__init__(name)

        # --- Robust logging setup ---
        log_file = Path(__file__).resolve().parent / "pipeline.log"
        formatter = logging.Formatter("%(asctime)s - %(message)s")

        if not getattr(self, "logger", None):
            self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        if not any(isinstance(h, logging.StreamHandler) for h in self.logger.handlers):
            ch = logging.StreamHandler()
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

        if not any(isinstance(h, logging.FileHandler) for h in self.logger.handlers):
            fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

        self.logger.info(f"Initialized step: {name}")
        self.logger.info(f"Logging to: {log_file}")

    def run(self):
        MB_CLEANSED_DIR.mkdir(parents=True, exist_ok=True)
        raw_files = sorted([f for f in MB_RAW_DIR.iterdir() if f.name in TSV_WHITELIST])

        if not raw_files:
            self.logger.warning("⚠️  No files found to clean.")
            return

        csv.field_size_limit(1_000_000)
        self.logger.info(
            f"🧹 Cleansing {len(raw_files)} TSV files from {MB_RAW_DIR} (ROW_LIMIT={ROW_LIMIT or '∞'})"
        )

        # ---- Main loop ----
        for infile in self.progress_iter(raw_files, desc="Cleansing TSVs"):
            # Ensure cleansed file has .tsv extension
            name = infile.name
            if "." not in name:
                name = f"{name}.tsv"
            outfile = MB_CLEANSED_DIR / name

            try:
                with open(infile, "r", encoding="utf-8", errors="replace") as fin, \
                     open(outfile, "w", encoding="utf-8", newline="") as fout:

                    reader = csv.reader(fin, delimiter="\t")
                    writer = csv.writer(fout, delimiter="\t")

                    headers = next(reader, [])
                    writer.writerow([h.strip() for h in headers])

                    row_count = 0
                    skipped = 0
                    total_rows = sum(1 for _ in open(infile, encoding="utf-8", errors="replace"))
                    effective_limit = ROW_LIMIT or total_rows

                    with tqdm(total=min(total_rows, effective_limit),
                              desc=infile.name[:30], leave=False) as bar:
                        for i, row in enumerate(reader, start=1):
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
                            if i % 100_000 == 0:
                                self.logger.info(f"{infile.name}: processed {i:,} rows")
                            bar.update(1)

                self.logger.info(
                    f"✅ Cleaned: {infile.name} → {outfile.name} "
                    f"({row_count:,} rows, {skipped:,} skipped)"
                )

            except Exception as e:
                self.logger.error(f"❌ Error processing {infile.name}: {e}")

        # ---- Post-processing ----
        self.logger.info(f"[DONE] Cleansed {len(raw_files)} TSV files → {MB_CLEANSED_DIR}")

        # ---- Derive release_year ----
        try:
            import re, pandas as pd

            possible_paths = [
                MB_CLEANSED_DIR / "release.tsv",
                MB_CLEANSED_DIR / "release"
            ]
            release_path = next((p for p in possible_paths if p.exists()), None)

            if not release_path:
                self.logger.warning("release.tsv not found in cleansed directory; skipping enrichment.")
                return

            self.logger.info(f"Found release file: {release_path}")

             # --- Read release.tsv safely whether headers exist or not ---
            import pandas as pd

            # Force no headers
            df = pd.read_csv(release_path, sep="\t", header=None, low_memory=False)

            # Try to assign official MusicBrainz release columns
            known_cols = [
                "id", "gid", "name", "artist_credit", "release_group",
                "status", "packaging", "language", "script",
                "barcode", "comment", "edits_pending", "quality",
                "last_updated"
            ]
            if len(df.columns) == len(known_cols):
                df.columns = known_cols
                self.logger.info("Assigned official MusicBrainz release headers (14 columns).")
            else:
                self.logger.warning(
                    f"Unexpected column count ({len(df.columns)}). Cannot apply official headers."
                )
                # fallback: create generic names
                df.columns = [f"col_{i}" for i in range(len(df.columns))]

            self.logger.info(f"Columns in release file: {list(df.columns)}")

            # --- Find date or year column automatically ---
            import re
            date_cols = [c for c in df.columns if re.search("date|year", c, re.I)]
            if not date_cols:
                # Try heuristic: any column containing a 4-digit year in first few rows
                sample = df.head(20)
                for c in df.columns:
                    if sample[c].astype(str).str.contains(r"\d{4}", regex=True).any():
                        date_cols.append(c)
                self.logger.info(f"Heuristic date columns: {date_cols}")

            if not date_cols:
                self.logger.warning("No date-like columns found; cannot derive release_year.")
                return

            # Pick first date-like column
            cand = df[date_cols[0]].astype(str)

            # --- Derive release_year ---
            def _coalesce_year(val):
                m = re.search(r"(\d{4})", str(val))
                return int(m.group(1)) if m else pd.NA

            df["release_year"] = cand.map(_coalesce_year).astype("Int64")

            out_path = MB_CLEANSED_DIR / "release_enriched.tsv"
            df.to_csv(out_path, sep="\t", index=False)

            coverage = df["release_year"].notna().mean()
            self.logger.info(f"✅ Added release_year → {out_path.name} (coverage={coverage:.1%})")

            # ---- Validation Preview ----
            self.logger.info("🔎 Validation: First 3 enriched rows:")
            cols = ["id", "release_year"] if "id" in df.columns else ["release_year"]
            for _, row in df[cols].head(3).iterrows():
                self.logger.info(f"   {row.to_dict()}")

        except Exception as e:
            self.logger.error(f"❌ Error deriving release_year: {e}")


if __name__ == "__main__":
    step = Step02CleanseTSV()
    step.run()

"""Step 00: Acquire MusicBrainz
Downloads and extracts the MusicBrainz full export.
Respects TSV_WHITELIST in config.py and saves raw TSVs to MB_RAW_DIR.
"""

from base_step import BaseStep
import re
import requests
import subprocess
import tarfile
from pathlib import Path
from bs4 import BeautifulSoup
from config import (
    CHUNK_SIZE,
    MB_DUMP_URL,
    MAX_RETRY_ATTEMPTS,
    MB_RAW_DIR,
    SEVEN_ZIP_PATH,
    TSV_WHITELIST,
)


class Step00AcquireMusicbrainz(BaseStep):
    FILENAME = "mbdump.tar.bz2"

    def __init__(self, name="Step 00: Acquire MusicBrainz", cleanup_archives: bool = True):
        super().__init__(name=name)
        self.cleanup_archives = cleanup_archives

    # ------------------------------------------------------------------
    def download_with_progress(self, url: str, dest_path: Path):
        """Download file with progress bar via BaseStep.progress_iter()."""
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total = int(response.headers.get("content-length", 0))
        chunk_size = max(1024, CHUNK_SIZE)
        desc = f"Downloading {self.FILENAME}"

        with open(dest_path, "wb") as file:
            for chunk in self.progress_iter(
                response.iter_content(chunk_size=chunk_size),
                desc=desc,
                unit="B",
                leave=True,
                total=total if total > 0 else None,
            ):
                if not chunk:
                    continue
                file.write(chunk)

    # ------------------------------------------------------------------
    def run(self):
        self.setup_logger()
        target_dir = MB_RAW_DIR
        seven_zip = SEVEN_ZIP_PATH
        target_dir.mkdir(parents=True, exist_ok=True)

        tar_bz2 = target_dir / self.FILENAME
        tar_path = target_dir / self.FILENAME.replace(".bz2", "")

        # ------------------------------------------------------------------
        # Step 0 – Safeguard: skip if all whitelisted TSVs already exist
        whitelist_stems = {Path(n).stem for n in TSV_WHITELIST}
        existing_stems = {f.stem for f in target_dir.glob("*.tsv")}
        missing = sorted(whitelist_stems - existing_stems)
        present = sorted(whitelist_stems & existing_stems)

        if len(present) == len(whitelist_stems):
            self.logger.info(f"✅ All whitelisted TSVs already exist in {target_dir}. Skipping Step 00.")
            return
        elif present:
            self.logger.info(f"⚠️ Partial TSVs found ({len(present)}/{len(whitelist_stems)}). Missing: {missing}")
        else:
            self.logger.info(f"⬇️ No whitelisted TSVs found — proceeding with acquisition.")

        # ------------------------------------------------------------------
        # Step 1 – Ensure we have a usable tar file
        if tar_path.exists():
            self.logger.info(f"Reusing existing tar archive: {tar_path}")
        elif tar_bz2.exists():
            self.logger.info(f"Extracting tar from existing {tar_bz2}...")
            subprocess.run([str(seven_zip), "x", str(tar_bz2), f"-o{target_dir}", "-y"], check=True)
        else:
            # Step 2 – Find latest dump folder
            self.logger.info("Checking MusicBrainz fullexport directory for latest dump...")
            with requests.get(MB_DUMP_URL) as response:
                soup = BeautifulSoup(response.text, "html.parser")
                folders = sorted(
                    [a["href"].strip("/") for a in soup.find_all("a", href=True)
                     if re.match(r"\d{8}-\d{6}", a["href"])],
                    reverse=True,
                )

            # Step 3 – Download mbdump.tar.bz2
            for folder in folders[:MAX_RETRY_ATTEMPTS]:
                url = f"{MB_DUMP_URL}{folder}/{self.FILENAME}"
                try:
                    self.logger.info(f"Trying download URL: {url}")
                    self.download_with_progress(url, tar_bz2)
                    break
                except requests.exceptions.HTTPError:
                    self.logger.warning(f"Not found: {url}")
            else:
                self.logger.error("No usable dump found in recent directories.")
                return

            subprocess.run([str(seven_zip), "x", str(tar_bz2), f"-o{target_dir}", "-y"], check=True)

        # ------------------------------------------------------------------
        # Step 4 – Extract whitelisted TSVs in one pass (normalize to stems)
        whitelist_stems = {Path(n).stem for n in TSV_WHITELIST}
        result = subprocess.run(
            [str(seven_zip), "l", str(tar_path)],
            capture_output=True, text=True, check=True,
        )
        lines = result.stdout.splitlines()
        internal_paths = {}
        for line in lines:
            parts = line.split()
            if len(parts) < 6:
                continue
            full_path = parts[-1]
            name = Path(full_path).name
            stem = Path(name).stem
            if stem in whitelist_stems:
                internal_paths.setdefault(stem, []).append(full_path)

        missing = [n for n in sorted(whitelist_stems) if n not in internal_paths]
        if missing:
            self.logger.warning(f"⚠️ Missing whitelisted TSVs: {missing}")

        extract_targets = []
        for paths in internal_paths.values():
            extract_targets.extend([f"-ir!{p}" for p in paths])

        if not extract_targets:
            self.logger.warning("⚠️ No whitelisted TSVs found to extract. Check TSV_WHITELIST.")
        else:
            cmd = [str(seven_zip), "e", str(tar_path), f"-o{target_dir}", "-y"] + extract_targets
            self.logger.info(f"Extracting {len(extract_targets)} TSV(s) from tar in one pass...")
            subprocess.run(cmd, check=True)
            self.logger.info(f"✅ Extraction complete. TSVs written to {target_dir}")

        # ------------------------------------------------------------------
        # Step 5 – Normalize file extensions (.tsv)
        for file in target_dir.iterdir():
            if file.is_file() and "." not in file.name and file.stem in whitelist_stems:
                new_name = file.with_suffix(".tsv")
                self.logger.info(f"Renaming {file.name} → {new_name.name}")
                file.rename(new_name)

        # ------------------------------------------------------------------
        # Step 6 – Basic validation
        try:
            with tarfile.open(tar_path, "r") as tf:
                found = any("release_first_release_date" in m.name for m in tf.getmembers())
                msg = "Found 'release_first_release_date' in tar archive." if found \
                    else "⚠️  'release_first_release_date' not found in tar archive."
                self.logger.info(msg)
        except Exception as e:
            self.logger.warning(f"Could not open tar for inspection: {e}")

        # ------------------------------------------------------------------
        # Step 7 – Cleanup (keep .tar for reuse)
        if self.cleanup_archives:
            try:
                tar_bz2.unlink()
                self.logger.info(f"Removed bz2 archive: {tar_bz2}")
            except FileNotFoundError:
                pass
            except Exception as e:
                self.logger.warning(f"Cleanup issue with {tar_bz2}: {e}")
        else:
            self.logger.info("Preserving both tar and bz2 archives per configuration.")

        self.logger.info("[DONE] Step 00 complete.")


if __name__ == "__main__":
    Step00AcquireMusicbrainz().run()

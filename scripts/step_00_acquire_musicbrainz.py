"""Step 00: Acquire MusicBrainz
Downloads and extracts the MusicBrainz full export.
Respects TSV_WHITELIST in config.py and saves raw TSVs to MB_RAW_DIR.
"""

from base_step import BaseStep
import os
import re
import requests
import subprocess
import tarfile
from pathlib import Path
from bs4 import BeautifulSoup
from config import MB_RAW_DIR, SEVEN_ZIP_PATH, TSV_WHITELIST
from tqdm import tqdm


class Step00AcquireMusicbrainz(BaseStep):
    BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/fullexport/"
    FILENAME = "mbdump.tar.bz2"

    def __init__(self, name="Step 00: Acquire MusicBrainz", cleanup_archives: bool = True):
        super().__init__(name="Step 00: Acquire MusicBrainz")
        self.cleanup_archives = cleanup_archives

    def download_with_progress(self, url: str, dest_path: Path):
        response = requests.get(url, stream=True)
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        with open(dest_path, "wb") as file, tqdm(
            desc="Downloading dump",
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=1024):
                size = file.write(chunk)
                bar.update(size)

    def run(self):
        target_dir = MB_RAW_DIR
        seven_zip = SEVEN_ZIP_PATH
        max_attempts = 10

        target_dir.mkdir(parents=True, exist_ok=True)

        # Log cleanup policy at the start
        self.logger.info(
            f"Archive cleanup policy: cleanup_archives={self.cleanup_archives}. "
            + ("Tarballs will be removed after extraction." if self.cleanup_archives else "Tarballs will be preserved.")
        )

        tar_bz2 = target_dir / self.FILENAME
        tar_path = target_dir / self.FILENAME.replace(".bz2", "")

        # Step 0: Ensure we have a usable tar file
        if tar_path.exists():
            self.logger.info(f"Found existing {tar_path}, reusing it.")
        elif tar_bz2.exists():
            self.logger.info(f"{tar_path} not found, extracting from {tar_bz2}...")
            subprocess.run([str(seven_zip), "x", str(tar_bz2), f"-o{target_dir}", "-y"], check=True)
            if not tar_path.exists():
                raise FileNotFoundError(f"Extraction failed: {tar_path} not created.")
        else:
            # Step 1: Find the latest dump folder
            self.logger.info("Checking MusicBrainz fullexport directory for latest dump...")
            with requests.get(self.BASE_URL) as response:
                soup = BeautifulSoup(response.text, "html.parser")
                folders = sorted(
                    [a["href"].strip("/") for a in soup.find_all("a", href=True)
                     if re.match(r"\d{8}-\d{6}", a["href"])],
                    reverse=True
                )

            # Step 2: Download mbdump.tar.bz2
            for folder in folders[:max_attempts]:
                url = f"{self.BASE_URL}{folder}/{self.FILENAME}"
                try:
                    self.logger.info(f"Trying download URL: {url}")
                    self.download_with_progress(url, tar_bz2)
                    self.logger.info(f"Downloaded dump to: {tar_bz2}")
                    break
                except requests.exceptions.HTTPError:
                    self.logger.warning(f"Not found: {url}")
            else:
                self.logger.error("No usable dump found in the last 10 attempts.")
                return

            # Extract .tar from .tar.bz2
            subprocess.run([str(seven_zip), "x", str(tar_bz2), f"-o{target_dir}", "-y"], check=True)

        # Step 3: List tar contents and match against whitelist
        whitelist = {Path(n).name for n in TSV_WHITELIST}
        result = subprocess.run(
            [str(seven_zip), "l", str(tar_path)],
            capture_output=True, text=True, check=True
        )
        lines = result.stdout.splitlines()

        internal_paths = {}
        for line in lines:
            parts = line.split()
            if len(parts) < 6:
                continue
            full_path = parts[-1]
            name = Path(full_path).name
            if name in whitelist:
                internal_paths.setdefault(name, []).append(full_path)

        missing = [name for name in sorted(whitelist) if name not in internal_paths]
        if missing:
            self.logger.warning(
                f"The following whitelisted TSVs were not found in the archive: {missing}"
            )

        # 🔎 Sanity log: which TSVs are actually being extracted
        self.logger.info(f"Whitelisted TSVs to extract: {sorted(list(whitelist))}")
        self.logger.info(f"TSVs found in archive and will be extracted: {sorted(list(internal_paths.keys()))}")

        # Step 4: Extract the whitelisted TSVs
        extracted_count = 0
        for name, paths in internal_paths.items():
            for pth in paths:
                self.logger.info(f"Extracting {name} from internal path: {pth}")
                subprocess.run(
                    [str(seven_zip), "e", str(tar_path), f"-o{target_dir}", f"-ir!{pth}", "-y"],
                    check=True
                )
                extracted_count += 1
        self.logger.info(f"Finished extracting {extracted_count} TSV(s) to {target_dir}")

        # Step 5: Sanity-check for release_first_release_date
        self.logger.info("Scanning for 'release_first_release_date'...")
        found_inflated = False
        for root, dirs, files in os.walk(target_dir):
            for fname in files:
                if "release_first_release_date" in fname:
                    self.logger.info(f"FOUND in extracted dir: {Path(root) / fname}")
                    found_inflated = True
        if not found_inflated:
            self.logger.warning("'release_first_release_date' not found in extracted files.")

        try:
            with tarfile.open(tar_path, "r") as tf:
                found_in_archive = False
                for member in tf.getmembers():
                    if "release_first_release_date" in member.name:
                        self.logger.info(f"Found inside archive: {member.name}")
                        found_in_archive = True
                if not found_in_archive:
                    self.logger.warning("'release_first_release_date' not found in tar archive.")
        except Exception as e:
            self.logger.warning(f"Could not open tar for inspection: {e}")

        # Step 6: Cleanup
        if self.cleanup_archives:
            for f in [tar_bz2, tar_path]:
                try:
                    f.unlink()
                    self.logger.info(f"Removed file: {f}")
                except Exception as e:
                    self.logger.warning(f"Cleanup issue with {f}: {e}")
        else:
            self.logger.info("Skipping cleanup of archives per configuration.")

        self.logger.info(f"[DONE] Step 00 complete.")


if __name__ == "__main__":
    step = Step00AcquireMusicbrainz()
    step.run()
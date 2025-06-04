# step_00_acquire_musicbrainz.py

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

    def __init__(self, name="Step 00 Acquire MusicBrainz", cleanup_archives: bool = True):
        super().__init__(name)
        # If True, remove downloaded .tar.bz2 and .tar after extraction
        self.cleanup_archives = cleanup_archives

    def download_with_progress(self, url: str, dest_path: Path):
        response = requests.get(url, stream=True)
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

        # Step 0: Find the latest dump folder
        self.logger.info("Checking MusicBrainz fullexport directory...")
        with requests.get(self.BASE_URL) as response:
            soup = BeautifulSoup(response.text, "html.parser")
            folders = sorted(
                [a["href"].strip("/") for a in soup.find_all("a", href=True)
                 if re.match(r"\d{8}-\d{6}", a["href"])],
                reverse=True
            )

        # Step 1: Download mbdump.tar.bz2 from the most recent folder available
        bz2_path = None
        for folder in folders[:max_attempts]:
            url = f"{self.BASE_URL}{folder}/{self.FILENAME}"
            candidate_path = target_dir / self.FILENAME
            try:
                self.logger.info(f"Trying download URL: {url}")
                self.download_with_progress(url, candidate_path)
                self.logger.info(f"Downloaded dump to: {candidate_path}")
                bz2_path = candidate_path
                break
            except requests.exceptions.HTTPError:
                self.logger.warning(f"Not found: {url}")
        else:
            self.logger.error("No usable dump found in the last 10 attempts.")
            return

        # Step 2: Extract the .tar from .tar.bz2
        tar_path = bz2_path.with_suffix("")  # strip .bz2 → mbdump.tar
        subprocess.run([str(seven_zip), "e", str(bz2_path), f"-o{target_dir}"], check=True)
        self.logger.info(f"Extracted tarball to: {tar_path}")

        # Step 3: List the tar contents and identify which whitelisted TSVs are present
        result = subprocess.run(
            [str(seven_zip), "l", str(tar_path)],
            capture_output=True, text=True, check=True
        )
        lines = result.stdout.splitlines()

        # Build a map: basename → list of full internal paths
        internal_paths = {}
        for line in lines:
            parts = line.split()
            if len(parts) < 6:
                continue
            full_path = parts[-1]
            name = Path(full_path).name
            if name in TSV_WHITELIST:
                internal_paths.setdefault(name, []).append(full_path)

        # Report any missing whitelisted files
        missing = [name for name in TSV_WHITELIST if name not in internal_paths]
        if missing:
            self.logger.warning(
                f"The following whitelisted TSVs were not found in the archive: {missing}"
            )

        # Step 4: Extract each whitelisted TSV by its internal path(s)
        extracted_count = 0
        for name, paths in internal_paths.items():
            for pth in paths:
                self.logger.info(f"Extracting {name} from internal path: {pth}")
                subprocess.run(
                    [str(seven_zip), "e", str(tar_path), f"-o{target_dir}", f"-ir!{pth}"],
                    check=True
                )
                extracted_count += 1

        self.logger.info(
            f"Finished extracting {extracted_count} TSV(s) to {target_dir}"
        )

        # Step 5: Inspect extracted directory for 'release_first_release_date'
        self.logger.info("Scanning extracted directory for 'release_first_release_date'...")
        found_inflated = False
        for root, dirs, files in os.walk(target_dir):
            for fname in files:
                if "release_first_release_date" in fname:
                    full_path = Path(root) / fname
                    self.logger.info(f"FOUND (inflated dir): {full_path}")
                    found_inflated = True
        if not found_inflated:
            self.logger.warning(
                "'release_first_release_date' not found in extracted files."
            )

        # Step 6: Inspect the tar for 'release_first_release_date'
        self.logger.info("Inspecting tar for 'release_first_release_date'...")
        try:
            with tarfile.open(tar_path, "r") as tf:
                found_in_archive = False
                for member in tf.getmembers():
                    if "release_first_release_date" in member.name:
                        self.logger.info(f"Found inside archive: {member.name}")
                        found_in_archive = True
                if not found_in_archive:
                    self.logger.warning(
                        "'release_first_release_date' not found inside the tar archive."
                    )
        except Exception as e:
            self.logger.warning(f"Could not open tar for inspection: {e}")

        # Step 7: Cleanup archives if requested
        if self.cleanup_archives:
            for f in [bz2_path, tar_path]:
                try:
                    f.unlink()
                    self.logger.info(f"Removed file: {f}")
                except Exception as e:
                    self.logger.warning(f"Cleanup issue with {f}: {e}")
        else:
            self.logger.info("Skipping cleanup of downloaded archives per configuration.")

        self.logger.info(f"[DONE] Step 00 complete.")

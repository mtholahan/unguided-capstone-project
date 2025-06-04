import re
import requests
import tarfile
import io
from bs4 import BeautifulSoup
import sys

BASE_INDEX = "https://data.metabrainz.org/pub/musicbrainz/data/fullexport/"
TARGET_FILENAME = "mbdump.tar.bz2"
FIND_NAME = "release_first_release_date"
HEARTBEAT_INTERVAL = 500  # print a heartbeat every N members


def get_recent_folders(max_folders=5):
    """Fetch the listing page and return the newest folder names (YYYYMMDD-HHMMSS)."""
    resp = requests.get(BASE_INDEX, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    folders = [
        a["href"].strip("/")
        for a in soup.find_all("a", href=True)
        if re.fullmatch(r"\d{8}-\d{6}/", a["href"])
    ]
    return sorted(folders, reverse=True)[:max_folders]


def check_folder_for_file(folder):
    """
    Stream-iterate through the compressed tarball from the given folder,
    looking for FIND_NAME in member paths, without downloading entire archive.
    Returns True if found, False if reached end without finding.
    """
    url = f"{BASE_INDEX}{folder}/{TARGET_FILENAME}"
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()

    member_count = 0
    try:
        with tarfile.open(fileobj=resp.raw, mode="r|bz2") as tar:
            for member in tar:
                member_count += 1
                if member_count % HEARTBEAT_INTERVAL == 0:
                    print(f"\r  Checked {member_count:,} entries...", end="", flush=True)
                if FIND_NAME in member.name:
                    print(f"\r  → Found after checking {member_count:,} entries: {member.name}")
                    return True
        print(f"\r  Checked {member_count:,} entries—{FIND_NAME} not found.")
        return False
    except (tarfile.ReadError, EOFError):
        print(f"\r  Stream ended after {member_count:,} entries without finding {FIND_NAME}.")
        return False


if __name__ == "__main__":
    recent = get_recent_folders(max_folders=10)
    for fol in recent:
        print(f"Checking {fol} ...")
        found = check_folder_for_file(fol)
        if found:
            print(f"Use this folder: {fol}")
            break
    else:
        print("None of the checked snapshots contained release_first_release_date.")
    sys.stdout.flush()

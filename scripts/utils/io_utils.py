import json
import shutil
from pathlib import Path
import re
import unicodedata

def atomic_write(path: str, data: str):
    tmp_path = Path(f"{path}.tmp")
    tmp_path.write_text(data, encoding="utf-8")
    shutil.move(tmp_path, path)


def safe_json_load(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def safe_json_dump(obj, path: str):
    atomic_write(path, json.dumps(obj, indent=2))


def normalize_for_matching_extended(s: str) -> str:
    """Normalize strings for fuzzy matching — strips accents, punctuation, etc."""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    return s.strip("_").lower()


def safe_filename(name: str, extension: str = ".json") -> str:
    """Return a safe filename for caching local JSON/API responses."""
    safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", name)
    if not safe.endswith(extension):
        safe += extension
    return safe


def write_json_cache(data: dict, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def read_json_cache(path: Path) -> dict | None:
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None

def ensure_dirs(paths):
    """
    Ensure one or more directories exist.
    Accepts a single Path or an iterable of Paths.
    Creates missing directories recursively and silently ignores existing ones.
    """
    if isinstance(paths, (str, Path)):
        paths = [paths]

    for p in paths:
        path = Path(p)
        try:
            path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"⚠️  Could not create directory {path}: {e}")

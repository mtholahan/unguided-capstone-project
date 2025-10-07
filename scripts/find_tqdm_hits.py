"""
find_tqdm_hits.py
Scans the repository for direct tqdm imports or calls,
excluding utils.py (the only permitted import location).
Integrates with Phase-1 literal audit conventions.
"""

import os
from utils import make_progress_bar  # ✅ unified progress bar helper

print("Current working directory:", os.getcwd())

# Project root (one level up from /scripts)
project_root = os.path.join(os.path.dirname(__file__), "..")

# Directories to skip (for performance + clarity)
EXCLUDE_DIRS = {
    ".venv",
    "__pycache__",
    ".git",
    "logs",
    "data",
    "output",
    "archive",
}

# Files to skip (case-insensitive)
EXCLUDE_FILES = {
    "find_tqdm_hits.py",  # this scanner
    "utils.py",           # ✅ only allowed tqdm import
}

# Simple substring patterns (faster than regex for this use)
TARGET_PATTERNS = [
    "from tqdm import",
    "tqdm(",
]

hits = []
py_files = []

# --- Collect candidate .py files --------------------------------------------
for root, dirs, files in os.walk(project_root):
    dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

    for f in files:
        if not f.lower().endswith(".py"):
            continue
        if f in EXCLUDE_FILES:
            continue
        py_files.append(os.path.join(root, f))

print(f"Scanning {len(py_files)} Python files under {project_root}...\n")

# --- Scan with progress bar --------------------------------------------------
for path in make_progress_bar(py_files, desc="Scanning files", unit="file"):
    try:
        with open(path, encoding="utf-8") as fh:
            for i, line in enumerate(fh, start=1):
                if any(p in line for p in TARGET_PATTERNS):
                    hits.append((path, i, line.strip()))
    except Exception as e:
        print(f"[WARN] Could not read {path}: {e}")

# --- Output summary ----------------------------------------------------------
if hits:
    print(f"\n🚫 Found {len(hits)} tqdm references outside utils.py:\n")
    for path, lineno, line in hits:
        print(f"{path}:L{lineno}  →  {line}")
else:
    print("\n✅ No direct tqdm usage found (all handled via utils.make_progress_bar).")

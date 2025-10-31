#!/usr/bin/env python3
"""
check_paths.py — diagnostic tool for verifying .env pipeline paths
Now with auto-creation of missing directories.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Resolve root path safely
PIPELINE_ROOT = Path(os.getenv("PIPELINE_ROOT", ".")).resolve()

# Derived directories
PIPELINE_OUTPUT_DIR = Path(os.getenv("PIPELINE_OUTPUT_DIR", PIPELINE_ROOT / "data" / "intermediate")).resolve()
PIPELINE_LOG_DIR = Path(os.getenv("PIPELINE_LOG_DIR", PIPELINE_ROOT / "logs")).resolve()
PIPELINE_METRICS_DIR = Path(os.getenv("PIPELINE_METRICS_DIR", PIPELINE_ROOT / "data" / "metrics")).resolve()
TMDB_RAW_DIR = Path(os.getenv("TMDB_RAW_DIR", PIPELINE_ROOT / "data" / "raw" / "tmdb_raw")).resolve()


# Display diagnostics
print("\n🔍 Pipeline Path Diagnostics")
print("=" * 60)
print(f"PIPELINE_ROOT       → {PIPELINE_ROOT}")
print(f"PIPELINE_OUTPUT_DIR → {PIPELINE_OUTPUT_DIR}")
print(f"PIPELINE_LOG_DIR    → {PIPELINE_LOG_DIR}")
print(f"PIPELINE_METRICS_DIR→ {PIPELINE_METRICS_DIR}")
print(f"TMDB_RAW_DIR        → {TMDB_RAW_DIR}")
print("=" * 60)

# Verify & create missing directories
print("\n📂 Directory existence check (auto-create enabled):")
for path in [PIPELINE_ROOT, PIPELINE_OUTPUT_DIR, PIPELINE_LOG_DIR, PIPELINE_METRICS_DIR, TMDB_RAW_DIR]:
    if not path.exists():
        try:
            path.mkdir(parents=True, exist_ok=True)
            print(f"{path} → 🆕 created")
        except Exception as e:
            print(f"{path} → ❌ failed to create ({e})")
    else:
        print(f"{path} → ✅ exists")

print("\n✅ Diagnostic and directory preparation complete.\n")

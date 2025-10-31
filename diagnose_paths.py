"""
TMDB/Discogs Path Diagnostic Script
Purpose: Identify misaligned or redundant folder structures (/data/data/... etc.)
Usage: python diagnose_paths.py
"""

import os
import sys
from pathlib import Path
from importlib import import_module

# --- Locate project root and add to path ---
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# --- Import env + utils (adapt if located elsewhere) ---
# from utils.env import get_env_path
# from scripts.utils.io_utils import ensure_dirs

# --- Import all step modules (adjust as needed) ---
STEP_MODULES = [
    "scripts_spark.spark_extract_tmdb",
    "scripts_spark.spark_extract_discogs",
    "scripts_spark.spark_prepare_tmdb_input",
    "scripts_spark.spark_validate_schema_alignment",
    "scripts_spark.spark_match_and_enrich",
]

import inspect

def safe_instantiate(module_name):
    """Instantiate step class safely and return its directory map."""
    try:
        mod = import_module(module_name)
        cls = next(getattr(mod, c) for c in dir(mod) if c.lower().startswith("step"))
        sig = inspect.signature(cls.__init__)
        kwargs = {}
        if "env_path" in sig.parameters:
            kwargs["env_path"] = ".env"
        obj = cls(**kwargs) if kwargs else cls()  # call with or without env_path
        return {
            "module": module_name,
            "class": cls.__name__,
            "root_dir": getattr(obj, "root_dir", None),
            "raw_dir": getattr(obj, "raw_dir", None),
            "intermediate_dir": getattr(obj, "intermediate_dir", None),
            "output_dir": getattr(obj, "output_dir", None),
            "cache_dir": getattr(obj, "cache_dir", None),
            "metrics_dir": getattr(obj, "metrics_dir", None),
        }
    except Exception as e:
        return {"module": module_name, "error": str(e)}


def main():
    print("\n=== PATH DIAGNOSTIC REPORT ===\n")
    print(f"Project Root: {ROOT}\n")

    for mod in STEP_MODULES:
        result = safe_instantiate(mod)
        if "error" in result:
            print(f"[ERROR] {mod}: {result['error']}")
        else:
            print(f"\n[{result['class']}] ({result['module']})")
            for k, v in result.items():
                if k not in ("module", "class") and v:
                    print(f"  {k:<18}: {v}")
    print("\n=== END OF REPORT ===")

if __name__ == "__main__":
    main()

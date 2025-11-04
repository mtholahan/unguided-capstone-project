# ================================================================
# inventory_pipeline_outputs.py
# ---------------------------------------------------------------
# Purpose: List all known pipeline output files and metadata
# Environment: Databricks Runtime 16.x+
# ================================================================

import os, json
from datetime import datetime
from pathlib import Path
import pandas as pd

try:
    dbutils  # noqa
except NameError:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

# --- Define known outputs (expand as needed) ---
KNOWN_OUTPUTS = {
    1: {"name": "tmdb_extract", "path": "dbfs:/mnt/raw/intermediate/tmdb_movies.csv"},
    2: {"name": "discogs_extract", "path": "dbfs:/mnt/raw/intermediate/discogs_albums.csv"},
    3: {"name": "tmdb_discogs_candidates", "path": "dbfs:/mnt/raw/intermediate/tmdb_discogs_candidates.csv"},
    4: {"name": "tmdb_discogs_candidates_extended", "path": "dbfs:/mnt/raw/intermediate/tmdb_discogs_candidates_extended.csv"},
    5: {"name": "tmdb_discogs_matches_v2", "path": "dbfs:/mnt/raw/intermediate/tmdb_discogs_matches_v2.csv"},
}

def get_dbfs_metadata(path: str):
    """Check existence & stats for a DBFS path."""
    try:
        base_dir = path.rsplit("/", 1)[0]
        for f in dbutils.fs.ls(base_dir):
            if f.name.strip("/").startswith(Path(path).name.split(".")[0]):
                return {
                    "exists": True,
                    "size_bytes": f.size,
                    "modified": datetime.utcfromtimestamp(f.modificationTime / 1000).isoformat()
                }
    except Exception:
        pass
    return {"exists": False, "size_bytes": None, "modified": None}


def run_inventory():
    print("🔎 Building pipeline output inventory...")

    records = []
    for step_no, meta in KNOWN_OUTPUTS.items():
        stats = get_dbfs_metadata(meta["path"])
        records.append({
            "step": step_no,
            "output_name": meta["name"],
            "path": meta["path"],
            "exists": stats["exists"],
            "size_bytes": stats["size_bytes"],
            "modified": stats["modified"],
        })

    df = pd.DataFrame(records)
    print("\n📋 Pipeline Output Inventory")
    display(df)

    out_path = "/dbfs/tmp/pipeline_inventory.csv"
    df.to_csv(out_path, index=False)
    print(f"📊 Inventory saved to: {out_path}")

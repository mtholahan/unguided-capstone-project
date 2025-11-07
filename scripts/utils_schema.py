"""
utils_schema.py – Shared schema validation and integrity utilities
-----------------------------------------------------------------
Supports Step 04 (Harmonized Data Validation & Schema Alignment)
Author: Springboard Data Bootcamp Coach
Version: v1.0 – Oct 2025
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

# ---------------------------
# Directory and logging helpers
# ---------------------------

def ensure_dirs(paths: List[Path]) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)


def setup_file_logger(logger: logging.Logger, logfile: Path) -> None:
    logfile.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    if all(
        not isinstance(h, logging.FileHandler) or getattr(h, "baseFilename", None) != str(logfile)
        for h in logger.handlers
    ):
        logger.addHandler(fh)


# ---------------------------
# Loaders and schema inference
# ---------------------------

def try_load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def iter_json_files(root: Path):
    if not root.exists():
        return []
    yield from root.rglob("*.json")


def safe_get(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    try:
        return d.get(key, default)
    except Exception:
        return default


def load_tmdb_fullscan(root: Path, logger: logging.Logger) -> pd.DataFrame:
    recs: List[Dict[str, Any]] = []
    for path in iter_json_files(root):
        data = try_load_json(path)

        # Handle list or dict
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("results", [])
        else:
            records = []

        for row in records:
            recs.append({
                "tmdb_id": safe_get(row, "id"),
                "title": safe_get(row, "title"),
                "release_date": safe_get(row, "release_date"),
                "genre_ids": safe_get(row, "genre_ids"),
                "popularity": safe_get(row, "popularity"),
                "vote_average": safe_get(row, "vote_average"),
                "vote_count": safe_get(row, "vote_count"),
                "source_file": path.name,
            })

    df = pd.DataFrame(recs)
    logger.info(f"Loaded TMDB: {len(df):,} rows from {root}")
    return df


def load_discogs_fullscan(root: Path, logger: logging.Logger) -> pd.DataFrame:
    recs: list[dict[str, Any]] = []

    for path in iter_json_files(root):
        data = try_load_json(path)

        # Handle list or dict structure
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("results", []) or data.get("filtered_results", [])
        else:
            records = []

        for row in records:
            recs.append({
                "discogs_id": safe_get(row, "id"),
                "title": safe_get(row, "title"),
                "year": safe_get(row, "year"),
                "genre": safe_get(row, "genre"),
                "style": safe_get(row, "style"),
                "country": safe_get(row, "country"),
                "source_file": path.name,
            })

    df = pd.DataFrame(recs)
    logger.info(f"Loaded Discogs: {len(df):,} rows from {root}")
    return df


def detect_dtype(series: pd.Series) -> str:
    pdt = str(series.dtype)
    if pdt == "object" and series.apply(lambda v: isinstance(v, (list, tuple))).mean() > 0.5:
        return "list"
    return pdt


def infer_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["column", "dtype", "non_null", "null_pct", "n_unique"])
    rows = []
    for col in df.columns:
        s = df[col]
        dtype = detect_dtype(s)
        # Safely compute unique count
        try:
            n_unique = int(s.nunique(dropna=True))
        except TypeError:
            # For unhashable types like lists
            n_unique = int(s.apply(lambda x: str(x)).nunique(dropna=True))
        rows.append({
            "column": col,
            "dtype": dtype,
            "non_null": int(s.notna().sum()),
            "null_pct": round(float(s.isna().mean()) * 100, 2),
            "n_unique": n_unique,
        })
    return pd.DataFrame(rows).sort_values("column").reset_index(drop=True)



# ---------------------------
# Integrity summary builder
# ---------------------------

def key_health(df: pd.DataFrame, key: str) -> Dict[str, Any]:
    total = len(df)
    nulls = df[key].isna().sum() if key in df.columns else total
    dupes = df[key].duplicated().sum() if key in df.columns else 0
    return {
        "key": key,
        "total": total,
        "nulls": int(nulls),
        "dupes": int(dupes),
        "null_pct": round((nulls / total * 100) if total else 0, 2),
        "dupe_pct": round((dupes / total * 100) if total else 0, 2),
    }


def build_integrity_summary(
    tmdb_df: pd.DataFrame,
    discogs_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    logger: "logging.Logger",
) -> pd.DataFrame:
    """
    Compute schema integrity summaries for TMDB, Discogs, and Candidates datasets.

    Notes:
        - Aligns to refactored schema using namespaced fields:
          tmdb_title, discogs_title, and *_title_norm.
        - Each record in the output reports % of null and duplicate values.
        - Skips any key that does not exist in the DataFrame to prevent KeyErrors.
    """
    rows: list[dict[str, any]] = []

    # --- TMDB & DISCOGS BRONZE DATASETS ---
    for dataset, df, keys in [
        ("tmdb", tmdb_df, ["tmdb_id", "tmdb_title"]),
        ("discogs", discogs_df, ["discogs_id", "discogs_title"]),
    ]:
        for k in keys:
            if k in df.columns:
                h = key_health(df, k)
                h.update({"dataset": dataset})
                rows.append(h)
            else:
                logger.warning(f"⚠️ Column '{k}' not found in {dataset} dataset; skipped.")

    # --- SILVER CANDIDATES DATASET ---
    if candidates_df is not None and not candidates_df.empty:
        logger.info(f"🧩 Processing candidates dataframe: {len(candidates_df):,} rows")
        candidate_keys = [
            "movie_ref",
            "tmdb_title_norm",
            "discogs_title_norm",
            "tmdb_title",
            "discogs_title",
        ]
        for k in candidate_keys:
            if k in candidates_df.columns:
                h = key_health(candidates_df, k)
                h.update({"dataset": "candidates"})
                rows.append(h)
            else:
                logger.debug(f"Skipping missing key '{k}' in candidates dataframe.")
    else:
        logger.warning("⚠️ No candidates data available; skipping integrity summary for that layer.")

    summary_df = pd.DataFrame(rows)
    summary_df = summary_df[["dataset", "key", "null_pct", "dupe_pct"]] if not summary_df.empty else summary_df
    return summary_df

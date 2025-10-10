# 🎬 Unguided Capstone – Discogs → TMDB ETL Prototype  
### Springboard Data Engineering Bootcamp · Step 5 Milestone  
*(Refactored October 10 2025 — Pivoted from MusicBrainz to Discogs)*

---

## 🧭 Project Overview

This repository contains the **prototype ETL pipeline** developed for the unguided capstone.  
The pipeline builds a bridge between **music metadata (Discogs)** and **film metadata (TMDB)**  
to explore the research question:

> **Does soundtrack genre impact a film’s popularity or rating?**

This Step 5 milestone demonstrates an end-to-end working ETL prototype that autonomously
acquires, cleans, joins, and validates data across two public APIs.  
Subsequent steps (6 → 10) will scale this prototype to cloud infrastructure and statistical analysis.

---

## 🧩 Rationale & Mid-Stream Pivot: *MusicBrainz → Discogs*

Originally, the project ingested data from **MusicBrainz**.  
However, during Step 4 development we discovered:

| Issue with MusicBrainz                      | Discogs Advantage                         |
| ------------------------------------------- | ----------------------------------------- |
| Limited or inconsistent soundtrack tagging  | Rich, explicit *genre* and *style* fields |
| Sparse linkage between releases and artists | Stable, queryable JSON API                |
| Weak genre normalization                    | Broad genre taxonomy usable for analytics |

**Decision:** pivot to **Discogs** for soundtrack acquisition while retaining the TMDB film endpoint.  
This change improved genre coverage, response speed, and data quality for downstream correlation.

---

## 🏗️ Repository Structure

```
unguided-capstone-project/
 │
 ├── data/
 │   ├── raw/              # raw Discogs and TMDB pulls
 │   ├── cache/            # cached API responses
 │   ├── intermediate/     # harmonized candidate pairs
 │   ├── processed/        # cleaned, matched datasets
 │   ├── metrics/          # run-level metrics JSON/CSV
 │   └── tmdb_enriched/    # TMDB enrichment outputs
 │
 ├── logs/                 # pipeline.log + per-step logs
 ├── scripts/              # all step_XX_*.py modules
 ├── docs/                 # README, summaries, changelog
 ├── evidence/             # screenshots, notebooks, validation reports
 ├── slides/               # presentation deck assets
 └── tmp/                  # transient local artifacts (git-ignored)
```



---

## ⚙️ Pipeline Architecture

```
Discogs → TMDB
 │
 ├── step_01_acquire_discogs.py     # Acquire soundtrack metadata via OAuth
 ├── step_02_fetch_tmdb.py          # Fetch movie metadata for candidate titles
 ├── step_03_prepare_tmdb_input.py  # Normalize & align title formats
 ├── step_04_match_discogs_tmdb.py  # Fuzzy + year-based matching
 └── main.py                        # Orchestrates Steps 1–4
```

Supporting modules:
- **utils.py** – request caching, rate limiting, unified logging  
- **base_step.py** – abstract class for step lifecycle control  
- **config.py** – environment variable and path management  

---

## 🔑 Key Design Features

- ✅ OAuth-based Discogs API access  
- ✅ Thread-safe `cached_request()` with consistent Response-like interface  
- ✅ Modular, class-driven ETL orchestration (`main.py`)  
- ✅ Structured logs and metrics (`logs/pipeline.log`, `data/metrics/`)  
- ✅ Verified semantic title matches (*A Star Is Born*, *Akira*, *12 Years a Slave*, *Black Panther*)  

---

## 📊 Validation Summary

| Metric                     | Result                                                       |
| -------------------------- | ------------------------------------------------------------ |
| Titles processed           | 200                                                          |
| Matched pairs (score ≥ 85) | **262 / 262 (100%)**                                         |
| Avg match score            | 90.0                                                         |
| Year alignment             | Δ ≤ 1 year for 92% of matches                                |
| Runtime                    | ≈ 3 min (local, 8 threads)                                   |
| Verified examples          | *12 Years a Slave*, *A Star Is Born*, *Akira*, *Alien*, *Black Panther* |

These results confirm that the pipeline reliably connects soundtrack releases to their corresponding films — producing believable, analysis-ready linkages.

---

## 🚀 Next Steps (Step 6 Preview)

1. Scale ETL to **Azure Blob Storage** for raw & processed data.  
2. Implement **Spark-based orchestration** for distributed matching.  
3. Compute correlations between Discogs genres and TMDB popularity / vote averages.  
4. Automate scheduled runs via Airflow or Azure Data Factory.

---

## 🧰 Local Execution

```bash
conda activate capstone
python main.py
```




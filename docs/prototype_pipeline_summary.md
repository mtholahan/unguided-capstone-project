# 🧩 Unguided Capstone – Step 5 Prototype Summary  
**Project:** Discogs → TMDB ETL Pipeline  
**Student:** Mark Holahan 
**Date:** October 10 2025  
**Mentor:** Akhil Raj

---

## 🎯 Objective
Design and implement a **prototype ETL pipeline** that automatically connects soundtrack releases from **Discogs** to their corresponding **films on TMDB**, producing a clean, analysis-ready dataset to explore the question:

> **Does soundtrack genre impact a film’s popularity or rating?**

This step demonstrates full ETL functionality on a local system, including data acquisition, normalization, matching, and validation.

---

## 🏗️ System Architecture

**Pipeline Flow**

```
Discogs → TMDB
 │
 ├── step_01_acquire_discogs.py      → OAuth acquisition & caching
 ├── step_02_fetch_tmdb.py           → Movie metadata retrieval
 ├── step_03_prepare_tmdb_input.py   → Normalize titles & prep candidates
 ├── step_04_match_discogs_tmdb.py   → Fuzzy title + year matching
 └── main.py                         → Orchestrates Steps 1–4
```

**Core Modules**

| File           | Purpose                                                 |
| -------------- | ------------------------------------------------------- |
| `utils.py`     | Unified request caching, rate-limiting, OAuth injection |
| `base_step.py` | Shared step lifecycle and structured logging            |
| `config.py`    | Environment & path configuration                        |
| `main.py`      | Entry point for full ETL run                            |

---

## 🔄 Data Sources

| Source      | API                          | Access Method               | Notes                                                 |
| ----------- | ---------------------------- | --------------------------- | ----------------------------------------------------- |
| **Discogs** | `https://api.discogs.com`    | OAuth (consumer key/secret) | Provides rich `genre`, `style`, and `year` fields     |
| **TMDB**    | `https://api.themoviedb.org` | API key                     | Provides `popularity`, `vote_average`, `release_date` |

---

## 🔁 Mid-Stream Pivot: *MusicBrainz → Discogs*

| Challenge with MusicBrainz  | Solution via Discogs                         |
| --------------------------- | -------------------------------------------- |
| Sparse soundtrack tagging   | Explicit “Soundtrack” genre and style fields |
| Strict 1 req/sec rate limit | Higher OAuth quota & faster response         |
| Complex pagination          | Simple page+per_page interface               |
| Inconsistent genre schema   | Well-maintained hierarchical genres          |

**Outcome:** richer, faster, and more reliable metadata for the prototype phase.

---

## 📊 Validation Results

| Metric                     | Value                                                        |
| -------------------------- | ------------------------------------------------------------ |
| Titles processed           | 200                                                          |
| Matched pairs (score ≥ 85) | **262 / 262 (100 %)**                                        |
| Avg match score            | 90.0                                                         |
| Year alignment             | Δ ≤ 1 year for > 90 % pairs                                  |
| Runtime                    | ~3 min (local, 8 threads)                                    |
| Example matches            | *12 Years a Slave*, *A Star Is Born*, *Akira*, *Black Panther*, *Alien* |

*Conclusion:* the ETL pipeline produces real, semantically valid soundtrack↔film connections with reproducible metrics and logs.

---

## 🧱 Repository Layout

```
unguided-capstone-project/
 │
 ├── data/
 │   ├── raw/            # Raw Discogs & TMDB pulls
 │   ├── cache/          # Cached API responses
 │   ├── intermediate/   # Harmonized candidate pairs
 │   ├── processed/      # Final matched datasets
 │   └── metrics/        # JSON & CSV summaries
 │
 ├── logs/               # pipeline.log + per-step logs
 ├── scripts/            # step_01–04_*.py modules
 ├── docs/               # README, this summary, changelog
 └── evidence/           # Validation notebooks & screenshots
```



---

## ✅ Rubric Alignment

| Requirement                   | Evidence                                                     |
| ----------------------------- | ------------------------------------------------------------ |
| **Autonomous orchestration**  | `main.py` runs Steps 1–4 sequentially                        |
| **Error handling & logging**  | `base_step.py` + unified logger in each step                 |
| **External data integration** | Discogs (OAuth) + TMDB (API key)                             |
| **Reproducibility**           | `.env` + `config.py` manage credentials & paths              |
| **Validation artifacts**      | `/logs/pipeline.log`, `/data/metrics/*.json`, `/evidence/validation.ipynb` |

---

## 🔮 Next Steps (Step 6 Preview)

1. **Scale** to Azure Blob Storage (raw + processed layers).  
2. **Distribute** processing with PySpark.  
3. **Analyze** correlation between soundtrack genre and TMDB popularity/vote average.  
4. **Deploy** orchestrated runs via Airflow or Azure Data Factory.

---

## 🪶 Submission Metadata

- **Branch:** `step5-submission`  
- **Tag:** `step5-milestone-2025-10-10`  
- **Reviewer:** Akhil Raj
- **Next Dev Branch:** `step6-dev`  

---

📁 **Included Artifacts**
- `main.py`, `utils.py`, `base_step.py`, `config.py`  
- `step_01–04_*.py`  
- `logs/pipeline.log`  
- `data/intermediate/discogs_tmdb_matches.csv`  
- `docs/README.md` & `docs/prototype_pipeline_summary.md`

---

*Prepared with guidance from Springboard Data Bootcamp Coach (GPT-5)*










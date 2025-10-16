# 🎬 Unguided Capstone – Discogs → TMDB ETL Prototype
### Springboard Data Engineering Bootcamp · Steps 5–6 Milestones  
*(Refactored October 2025 — Local Spark Baseline + Stable Environment)*

---

## 🧭 Project Overview

This repository contains the **prototype + scaling ETL pipeline** developed for the unguided capstone.  
The pipeline bridges **music metadata (Discogs)** and **film metadata (TMDb)** to explore:

> **Does soundtrack genre impact a film’s popularity or rating?**

Step 5 established a functional local ETL;  
Step 6 scales the matching logic to **Apache Spark** using a stable PySpark–Pandas stack verified on Ubuntu WSL 2 and ready for Azure deployment.

---

## 🧩 Mid-Stream Pivot: *MusicBrainz → Discogs*

| Issue with MusicBrainz                     | Discogs Advantage                   |
| ------------------------------------------ | ----------------------------------- |
| Sparse or inconsistent soundtrack tagging  | Explicit *genre* and *style* fields |
| Limited linkage between releases ↔ artists | Robust JSON API with stable IDs     |
| Weak genre normalization                   | Broad taxonomy useful for analytics |

**Decision:** pivot to Discogs to improve genre coverage, speed, and data quality for downstream correlation.

---

## 🏗️ Repository Structure

```
unguided-capstone-project/
├── data/
│ ├── raw/ # API pulls
│ ├── cache/ # cached JSON
│ ├── intermediate/ # harmonized candidate pairs
│ ├── processed/ # cleaned, matched data
│ ├── metrics/ # run-level metrics
│ └── tmdb_enriched/
├── logs/ # pipeline + Spark logs
├── scripts/ # step_XX_*.py modules
├── docs/ # readme, changelog, notes
├── evidence/ # screenshots, validation
├── slides/ # presentation deck
├── rebuild_venv.sh # reproducible environment script
├── requirements_stable.txt
└── tmp/ # transient artifacts (git-ignored)
```



---

## ⚙️ Pipeline Architecture

```
Discogs → TMDB
│
├── step_01_acquire_discogs.py
├── step_02_fetch_tmdb.py
├── step_03_prepare_tmdb_input.py
├── step_04_match_discogs_tmdb.py
├── step_05_prototype_pipeline.py
└── step_06_scale_prototype.py ← PySpark scaling + metrics
```



### Supporting modules:

- **utils.py** – request caching, rate limiting, logging  
- **base_step.py** – step lifecycle base class  
- **config.py** – environment and path management  

---

## 🔑 Key Design Features

- ✅ OAuth Discogs API access  
- ✅ Modular ETL orchestration (`main.py`)  
- ✅ Thread-safe caching & logging  
- ✅ Spark UDF for hybrid fuzzy matching (RapidFuzz + year logic)  
- ✅ Automatic metrics + plots (JSON + PNG)  
- ✅ Fully reproducible environment via `rebuild_venv.sh`

---

## 📊 Validation Snapshot (Step 5)

| Metric                     | Result                     |
| -------------------------- | -------------------------- |
| Titles processed           | 200                        |
| Matched pairs (score ≥ 85) | **262 / 262 (100 %)**      |
| Avg match score            | 90.0                       |
| Year alignment Δ           | ≤ 1 year for 92 %          |
| Runtime                    | ≈ 3 min (local, 8 threads) |

---

## ⚙️ Environment Setup (Stable Baseline for Step 6 → Azure)

This project runs on:
- **Python 3.11**
- **PySpark 3.5.2**
- **Pandas 2.0.3**
- **NumPy 1.26.4**
- **Ubuntu WSL 2 or native Linux**

### 1️⃣ Rebuild the Environment

```shell
chmod +x rebuild_venv.sh
./rebuild_venv.sh
```

Creates a fresh venv at ~/pyspark_venv311, installs all pinned packages,
and auto-generates requirements_stable.txt.

To activate later:

```bash
source ~/pyspark_venv311/bin/activate
```

2️⃣ Configure VS Code (Optional)

```bash
Ctrl + Shift + P → Python: Select Interpreter → /home/mark/pyspark_venv311/bin/python
```

3️⃣ Run the Spark Step Locally

```bash
cd /mnt/c/Projects/unguided-capstone-project
source ~/pyspark_venv311/bin/activate
python scripts/step_06_scale_prototype.py
```

Outputs:

- data/intermediate/tmdb_discogs_matches_spark.csv

- data/metrics/step06_spark_metrics.json

- data/metrics/step06_spark_score_distribution.png


4️⃣ Deploy to Azure (Next Step)
On your Azure Spark cluster or VM:

```bash
pip install -r requirements_stable.txt
spark-submit scripts/step_06_scale_prototype.py
```

Ensures identical dependencies between local and cloud environments.

- 💡 Notes
  ❗ Avoid pip freeze > requirements.txt — it captures dev-tools and may upgrade core libs.
- The pinned versions above are the last known good combo avoiding Pandas _new_Index serialization issues.

- If upgrading Spark → 4.x, revisit the Pandas UDF patch inside step_06_scale_prototype.py.


🗺️ Environment Diagram (Conceptual)

```
graph TD
    A[rebuild_venv.sh] --> B[pyspark_venv311]
    B --> C[VS Code Interpreter]
    B --> D[requirements_stable.txt]
    C --> E[Local Spark Job]
    D --> F[Azure Cluster (spark-submit)]
```

1. 🚀 Next Steps
   Upload data & outputs to Azure Blob Storage.

2. Execute Step 07 (Deploy Spark Job on Azure).

3. Perform Step 08–10 statistical analysis and visualization.

4. Finalize capstone submission with evidence artifacts.


© 2025 Mark — Springboard Data Engineering Bootcamp






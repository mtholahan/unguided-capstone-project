# Unguided Capstone – TMDB + Discogs Data Pipeline  
**Version 1.8.0  |  Step 8 – Deploy for Testing  |  Status:** 🟩 Active  |  Branch:** `step8-dev`**

**Mentor:** Akhil  
**Sprint Window:** Oct 17 – Oct 31 2025  

---

## 🎯 Project Overview
This unguided capstone integrates two open entertainment datasets — **The Movie Database (TMDB)** and **Discogs** — into a unified, scalable analytics pipeline.  
The goal is to demonstrate production-grade **data engineering** across ingestion, transformation, orchestration, and testing using **PySpark** and **Azure Databricks**.

By Step 8, the project transitions from *architecture design* (Step 7) to *operational validation* — deploying refactored Spark modules in Azure and confirming correctness through automated testing.

---

## 🧩 Problem Statement
Entertainment metadata lives in silos: TMDB tracks films, Discogs catalogs music.  
Cross-domain analytics (e.g., *film–soundtrack linkage*) require merging both ecosystems.  
This pipeline builds a reproducible, cloud-scale workflow to ingest, transform, and align these datasets for analytical exploration.

---

## ⚙️ Technical Objectives
- Maintain modular extractors for TMDB and Discogs APIs.  
- Refactor legacy Python (steps 03–05) to **PySpark 3.5.x** for distributed execution.  
- Integrate **pytest + coverage** to validate data flow and transformation logic.  
- Deploy and execute on **Azure Databricks Runtime 14.3 LTS (Spark 3.5.0)**.  
- Persist results to **Azure Data Lake Storage Gen2** via **ABFSS URIs**.  
- Confirm end-to-end reproducibility and test pass rates in both local and cloud environments.

---

## 🏗️ Phase Recap (1–7 Completed)
| Step | Focus                                 | Outcome                                 |
| ---- | ------------------------------------- | --------------------------------------- |
| 1    | Project definition & data exploration | Problem charter, API survey             |
| 2    | API extraction (TMDB & Discogs)       | Working extract scripts                 |
| 3    | Data preparation                      | Schema mapping prototype                |
| 4    | Validation                            | Cross-schema alignment checks           |
| 5    | Matching & Enrichment                 | Fuzzy-matching prototype                |
| 6    | Scaling Prototype                     | Spark 3.5 baseline + Databricks cluster |
| 7    | Deployment Architecture               | Azure Bicep IaC validated (`what-if`)   |

**Step 7 Outcome:**  
All Azure components (networking, Key Vault, Storage, Databricks, Function App, Monitoring) deployed via Bicep and verified cost-neutral through CLI what-if tests.

---

## 🚀 Step 8 – Deploy Your Code for Testing (Active Phase)

### 🎯 Purpose
Stabilize and validate the Spark-refactored pipeline inside Azure.  
This phase proves functional parity between local and cloud runs, implements pytest coverage, and finalizes module interfaces for orchestration.

### 🧱 Deliverables
- Refactored Spark modules:  
  `prepare_spark_tmdb_input.py`, `validate_spark_schema.py`, `match_spark_enrich.py`  
- Automated test suite (`pytest + pytest-cov`) covering Steps 01–05.  
- Integration execution on Databricks workspace (`capstone-blob-cluster`).  
- Coverage & metrics reports (exported to /metrics/).  
- Updated slide deck with testing summary.

### 🧩 Environment Summary
| Component            | Specification                                              |
| -------------------- | ---------------------------------------------------------- |
| Databricks Workspace | Deployed via `databricks.bicep`                            |
| Cluster Name         | `capstone-blob-cluster`                                    |
| Runtime              | 14.3 LTS – Apache Spark 3.5.0 / Scala 2.12                 |
| Node Type            | Standard_D4ps_v6 (16 GB RAM, 4 Cores, Single Node)         |
| Termination Policy   | Auto-terminate after 30 min idle                           |
| Storage Access       | `abfss://raw@markcapstoneadls.dfs.core.windows.net/`       |
| Local Parity         | Ubuntu + `pyspark_venv311` (PySpark 3.5.2, Python 3.11.14) |

---

### 💻 Environment Setup Guide

#### **1️⃣ Local PySpark (Development / Testing – $0)**
**Purpose: Fast iteration and pytest coverage before cloud deployment.**  

```bash
# Activate venv
source ~/pyspark_venv311/bin/activate
```



```bash
# Verify Spark
python -c "from pyspark.sql import SparkSession; print(SparkSession.builder.master('local[2]').getOrCreate())"
```



```bash
# Install dependencies (if needed)
pip install -p pyspark pytest pytest-cov rapidfuzz
```



```bash
# Run unit tests with coverage
pytest -q --cov=scripts_spark --cov-report=term-missing
```

Outputs: Parquet files → data/intermediate/ and coverage report in console.

#### 2️⃣ Azure Databricks (Validation / Integration – $$)

**Purpose: Execute identical modules under production-grade Spark cluster.**

- Start cluster `capstone-blob-cluster` (Runtime 14.3 LTS).

- In Databricks Repos → Sync branch step8-dev.

- Open notebook and run:


```python
from scripts_spark.prepare_spark_tmdb_input import run_step
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
config = {"input_root": "abfss://raw@markcapstoneadls.dfs.core.windows.net/"}
df = run_step(spark, config)
df.show(5)
```

- Validate ADLS writes and pytest results within cluster.

- Shut down cluster after testing to avoid idle billing.


### 🧪 Testing & Validation Workflow

1. Execute local pytest to verify module interfaces.
2. Deploy and re-run tests in Databricks (coverage ≥ 80 %).

3. Inspect metrics JSON under /data/metrics/ for record counts and durations.

4. Update slide deck with coverage and test results screenshots.


Sample pytest command:

```python
pytest -q --disable-warnings --maxfail=1 --cov=scripts_spark --cov-report=term-missing
```

### 📂 Repository Structure (Updated)

```
project-root/
├── scripts/ # Legacy Python steps (01–06)
├── scripts_spark/ # Refactored Spark modules (03–05)
│   ├── extract_spark_tmdb.py
│   ├── extract_spark_discogs.py
│   ├── prepare_spark_tmdb_input.py
│   ├── validate_spark_schema.py
│   └── match_spark_enrich.py
├── data/ (intermediate · metrics)
├── infrastructure/ (Bicep templates)
├── tests/ (pytest suites)
├── slides/ (Step 8 Testing Deck)
└── README.md
```

### 🧭 Development Modes Recap

| Mode                       | Description                     | Usage                              |
| -------------------------- | ------------------------------- | ---------------------------------- |
| **Local (Ubuntu PySpark)** | VS Code + `pyspark_venv311`     | Development + pytest               |
| **Azure Databricks**       | Cluster `capstone-blob-cluster` | Integration + validation           |
| **Azure IaC Layer**        | Bicep templates                 | Infrastructure already provisioned |

### 🧾 Status & Metadata

- Current Step: 8 – Deploy for Testing

- Mentor: Akhil

- Active Branch: step8-dev

- Next Milestone: Step 9 – Scale Your Prototype (Performance Optimization)

- Primary Author: M. Holahan

- Last Updated: Oct 23 2025



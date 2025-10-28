# Unguided Capstone – TMDB + Discogs Data Pipeline  
**Version 2.0.0  |  Step 8 – Deploy for Testing  |  Status:** 🟩 Active  |  Branch: `step8-dev`  

**Mentor:** Akhil  
**Sprint Window:** Oct 17 – Oct 31 2025  

---

## 🎯 Project Overview
This capstone unifies **The Movie Database (TMDB)** and **Discogs** datasets into a production-grade analytics pipeline.  
It demonstrates the full data engineering lifecycle — ingestion, transformation, orchestration, and testing — using **PySpark 3.5.x** and **Azure Databricks**.

By Step 8, the project achieves operational stability: local and cloud environments mirror each other exactly, and the entire Spark pipeline can be rebuilt, tested, and deployed deterministically.

---

## ⚙️ Technical Objectives
- Maintain modular ETL design with refactored Spark modules  
- Integrate **pytest + coverage** for transformation validation  
- Deploy to **Azure Databricks Runtime 14.3 LTS** and confirm reproducibility  
- Automate environment synchronization between **Local ↔ VM ↔ Git**  
- Track every rebuild and validation event in `sync_log.md`  

---

## 🧱 Infrastructure Summary

| Layer | Tooling | Purpose |
|-------|----------|----------|
| Compute | Azure VM + Databricks | Local Spark development and cloud validation |
| Storage | Azure Data Lake (ABFSS) | Raw + processed datasets |
| IaC | Bicep templates | Declarative, reproducible infrastructure |
| Tracking | sync_log.md | Chronological record of environment syncs |
| Testing | pytest + coverage | Automated data validation and coverage analysis |

---

## 🚀 Operational Runbook

### 1️⃣ **Activate the Environment**
```bash
source ~/pyspark_venv311/bin/activate
```

---

### 2️⃣ **Anchor → Execute → Validate**
Run the complete environment synchronization and pipeline execution loop:
```bash
make sync
```

This sequence:
1. Pulls latest Git changes  
2. Rebuilds your local venv  
3. Exports requirements to Azure VM  
4. Rebuilds VM venv remotely  
5. Validates both environments  
6. Executes the Spark pipeline safely  

✅ **Result:** Full local/VM parity confirmed, and a timestamped entry is logged in `sync_log.md`.

---

### 3️⃣ **Dry Run (Validation Only)**
```bash
make dryrun
```
Verifies all configurations, dependencies, and environment variables without executing Spark.

---

### 4️⃣ **Clean Workspace**
```bash
make clean
```
Removes cache files, logs, and any outdated virtual environments.

---

### 5️⃣ **Review Sync History**
```bash
make log
```
Displays the latest entries in your environment sync log for audit and traceability.



> 📘 **Need help running or debugging the pipeline?**  
> Check out the [Azure Pipeline Run Diagnostics Guide](docs/pipeline_run_diagnostics.md)



---

## 🧩 Key Auxiliary Scripts

| Script | Purpose | Relation |
|--------|----------|----------|
| `rebuild_venv.sh` | Rebuilds or refreshes the local virtual environment; exports requirements to VM | Core of rebuild process; invoked by `make sync` |
| `check_env.sh` | Verifies Spark, Python, and Azure configuration before any run | Used by `rebuild_venv.sh` and `run_pipeline_safe.sh` |
| `run_pipeline_safe.sh` | Safely launches the Spark pipeline after pre-flight checks | Depends on `check_env.sh` |
| `Makefile` | Central automation hub for rebuild, export, sync, test, and clean | Controls full workflow |
| `.env` | Defines environment variables for Spark and Azure | Loaded by all scripts |
| `requirements_stable.txt` | Canonical dependency list for reproducible rebuilds | Synced between local and VM |
| `sync_log.md` | Chronological log of environment syncs and validation events | Auto-updated after each rebuild |

🟩 **Safe to delete:** `deploy_to_azure_test.sh` — its functionality is fully replaced by `make sync`.

---

## 🧪 Testing & Validation Workflow
```bash
pytest -q --disable-warnings --maxfail=1 --cov=scripts_spark --cov-report=term-missing
```
Outputs:
- Console coverage summary  
- `/data/metrics/` JSON with record counts and durations  

🎯 **Target Coverage:** ≥ 80%

---

## 📂 Repository Structure

```
unguided-capstone-project/
├── scripts/                 # Legacy steps 01–06
├── scripts_spark/           # Refactored Spark modules (03–05)
├── data/                    # Intermediate + metrics data
├── infrastructure/          # Bicep templates
├── tests/                   # pytest suites
├── logs/                    # Run logs
├── sync_log.md              # Environment sync history
├── Makefile                 # Unified automation commands
├── rebuild_venv.sh          # Environment rebuild utility
├── run_pipeline_safe.sh     # Spark pipeline launcher
├── check_env.sh             # Pre-flight diagnostic tool
└── README.md
```

---

## 🧭 Development Modes

| Mode | Description | Usage |
|------|--------------|--------|
| **Local (Ubuntu + PySpark)** | Fast iteration and pytest validation | Development + coverage |
| **Azure VM** | Mirrors local environment for production-like validation | End-to-end testing |
| **Azure Databricks** | Scaled Spark validation | Integration + performance testing |

---

## ✅ Status & Metadata
- **Current Step:** 8 – Deploy for Testing  
- **Next Step:** 9 – Scale Your Prototype  
- **Mentor:** Akhil  
- **Active Branch:** step8-dev  
- **Primary Author:** M. Holahan  
- **Last Updated:** Oct 28, 2025  

---

_This README marks the stabilization of the Unguided Capstone’s operational environment. All infrastructure, dependencies, and orchestration systems are now deterministic, documented, and reproducible._

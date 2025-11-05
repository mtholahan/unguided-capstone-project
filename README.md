# Unguided Capstone – TMDB + Discogs Data Pipeline  
**Version 2.0.0  |  Step 8 – Deploy for Testing  |  Status:** 🟩 Active  |  Branch: `step8-dev`  

**Mentor:** Akhil  

---

## 🎯 Project Overview
This capstone integrates data from **The Movie Database (TMDB)** and **Discogs** into a
scalable Spark-based pipeline built on **PySpark 3.5** within **Azure Databricks**.  
The project implements the core data-engineering lifecycle—data ingestion,
transformation, and validation—using modular PySpark components designed for future
production deployment.

By the end of **Step 8**, the pipeline demonstrates full runtime stability within
Databricks: configuration management, Spark session initialization, and data-path
validation all operate successfully inside a controlled testing environment.
These validations establish the foundation for the production-scale execution and
storage integration that will be completed in **Step 9**.

---

## 📚 Data Sources
- **TMDB API:** metadata for movies  
- **Discogs API:** catalog and release data for artists and recordings  
Combined, these datasets enable cross-domain analytics linking filmography and discography metadata.



------

## ⚙️ Technical Objectives

- Maintain modular ETL design across TMDB + Discogs sources  
- Operate exclusively in **Azure Databricks Runtime 16 LTS**  
- Ensure deterministic rebuild and reproducibility across environments  
- Validate Spark session, configuration, and I/O integration  

---

## 🏗️ Architecture Overview
The current architecture runs entirely on **Azure Databricks**, following the Step 7 design with the
**Azure Data Factory component removed** at the mentor’s request.  
Data ingestion, transformation, and validation all occur within Databricks notebooks using
Azure Data Lake for storage.

<p align="center">
  <img src="architecture/diagrams/ungcap_architecture_step8.png" width="720" alt="Step 8 Architecture Diagram – Databricks-Only Pipeline">
</p>

------



## 🧰 Project Setup (Databricks)

1. **Import the project**  
   Upload the repository to your Databricks workspace under  
   `/Workspace/Users/<username>/unguided-capstone-project/`.

2. **Attach a cluster**  
   - Runtime: Databricks 16 LTS (Python 3.11, Spark 3.5.x)  
   - Libraries: `pyspark`, `requests`, `pandas`, `dotenv`  

3. **Configure environment variables**  
   - Optional: `TMDB_API_KEY`, `DISCOGS_TOKEN`  
   - Set them in the cluster environment or notebook scope.

---

## 🚀 Running the Pipeline

1. **Open** `Pipeline_Runner.ipynb`  
2. **Run All Cells** to execute ingestion → transformation → load  
3. Review output logs under `/Workspace/Users/.../logs/`

For quick health verification, use `Testing.ipynb`  
(the **Step 8 Validation Cell**) to confirm that configuration,
Spark session, and directory structure initialize correctly.

---

## 🧪 Testing & Validation Workflow

Testing for Step 8 was transitioned from a Databricks notebook harness (`Testing.ipynb`) to a formal **PyTest-based suite** that can be executed locally or within a Databricks Repo context.
 This suite provides rubric-compliant **unit test metrics and coverage reporting** for configuration, Spark initialization, and environment edge-cases.

**Test Suite Summary**

| Category               | Validation Target                                 | Result   |
| ---------------------- | ------------------------------------------------- | -------- |
| Config Import          | `config.py` loads constants, local + ADLS paths   | ✅ Passed |
| Spark Session          | Local Spark session initializes via fixture       | ✅ Passed |
| Path Validation        | Confirms expected directories and I/O paths exist | ✅ Passed |
| Edge Case (Empty DF)   | Empty DataFrame creates + counts 0 rows           | ✅ Passed |
| Edge Case (Config Key) | Invalid config key raises `KeyError`              | ✅ Passed |

**Execution Metrics**

| Metric         | Value                                                        |
| -------------- | ------------------------------------------------------------ |
| Tests Executed | 6                                                            |
| Tests Passed   | 6                                                            |
| Tests Failed   | 0                                                            |
| Code Coverage  | ≈ 90 % (total files), 100 % for core fixtures and config tests |

**Implementation Notes**

- PyTest fixtures (`conftest.py`) initialize an isolated Spark session and dynamically import `config.py` without Databricks dependencies.
- The suite runs identically in both Databricks Repos and local environments using `pytest -v` and `pytest --cov`.
- A coverage summary (`test_report.txt`) is committed with this branch.
- The new test suite supersedes the earlier notebook-based validation cell; future steps (Step 9 – production deployment) will extend these tests to include data-level validation and output integrity.

---

## 📂 Repository Structure

```
unguided-capstone-project/
├── notebooks/                     # Databricks notebooks (runtime + validation)
│   ├── Pipeline_Runner.ipynb      # Main ETL entrypoint
│   ├── Testing.ipynb              # Step 8 validation harness
│   └── Data_Inspection.ipynb      # Exploratory data checks
│
├── scripts/                       # Core ETL and utilities
│   ├── config.py                  # Central configuration
│   ├── extract_spark_tmdb.py      # TMDB ingestion
│   ├── extract_spark_discogs.py   # Discogs ingestion
│   ├── match_and_enrich.py        # Record matching + enrichment
│   ├── prepare_tmdb_discogs_candidates.py
│   ├── inventory_pipeline_outputs.py
│   ├── utils.py / utils_schema*.py / validate_schema_alignment.py
│   └── bootstrap.py               # Spark session + environment setup
│
├── data/                          # Staging + processed data directories
│   ├── raw/ | processed/ | intermediate/ | validation/
│   └── metrics/ | logs/ | cache/
│
├── logs/                          # Runtime logs (pipeline + validation)
├── architecture/diagrams/         # Architecture diagrams (Step 7 → Step 8)
├── docs/                          # Mentor + ops documentation
├── infrastructure/                # Archived IaC (Step 7 artifacts)
├── slides/                        # Presentation decks
├── requirements*.txt / pyproject.toml
└── README.md

```

> [!NOTE]
>
> Directories under `infrastructure/` and some archived scripts are retained for historical reference but are not active in the current Databricks-only workflow.
>


### 📘 Notes
- The **Databricks notebooks** (`Pipeline_Runner.ipynb` and `Testing.ipynb`) now serve as the operational and validation entrypoints for Step 8 onward.  
- The **infrastructure/** directory represents Step 7 (architecture diagram + IaC) and is not executed in Step 8.  
- The **scripts/** directory is the active codebase for all pipeline logic validated during Step 8 testing.  

---

### 🔄 Transition to Step 9
The **PyTest validation suite** introduced in Step 8 now provides a reproducible regression-testing framework for subsequent deployment stages.
With all six tests passing and ~90 % coverage, the pipeline’s configuration, Spark initialization, and I/O logic are verified as stable across both local and Databricks environments.
Step 9 will reuse this same environment and test suite while scaling to process the full TMDB + Discogs datasets and persist outputs to Azure Storage.
No code refactoring is required—only cluster and data-volume scaling—allowing Step 9 to focus on production deployment validation, output integrity, and documentation.



------

------

## **Project:** Unguided Capstone – TMDB + Discogs Data Pipeline  **Version:** 1.7.0 (Step 7: Deployment Architecture)  **Status:** 🟩 Active  **Branch:** `step7-dev`  **Sprint Window:** Oct 17 – Oct 31, 2025  **Mentor:** Akhil (Recoupling post-pause)

# Unguided Capstone Project – TMDB + Discogs Data Engineering Pipeline

*Springboard Data Engineering Bootcamp – Unguided Capstone*

------

## 🎯 Project Overview

This unguided capstone project unifies two open data ecosystems — **The Movie Database (TMDB)** and **Discogs** — to design and deploy a scalable data pipeline capable of ingesting, transforming, and serving structured entertainment metadata for analytics.

The pipeline demonstrates end-to-end data engineering competency across extraction, transformation, orchestration, and cloud deployment, integrating both **PySpark-based ETL** and **Azure-native infrastructure**.

------

## 🧩 Problem Statement

Entertainment metadata is highly fragmented across sources. TMDB specializes in film data; Discogs curates music metadata. Analytical scenarios (e.g., soundtrack correlation or cross-domain artist appearances) require unified datasets. This project builds a reproducible, cloud-scalable pipeline to integrate, process, and expose TMDB and Discogs data for such analysis.

------

## ⚙️ Technical Objectives

- Design modular extractors for both TMDB and Discogs APIs.
- Implement a PySpark-based transformation pipeline for schema harmonization.
- Develop scalable orchestration patterns using Databricks and Azure components.
- Introduce Infrastructure-as-Code (IaC) to define deployment topology.
- Validate end-to-end reproducibility through testing and version control.

------

## 🏗️ Architecture Summary

### Phase 1–6: Data Pipeline Development (Completed)

**Core stack:** Python · PySpark · Databricks · Azure Blob Storage · Key Vault

| Step | Focus                                 | Deliverable                                              |
| ---- | ------------------------------------- | -------------------------------------------------------- |
| 1    | Define problem, scope, and objectives | Project charter & dataset exploration                    |
| 2    | API Exploration & Ingestion           | Raw extract scripts for TMDB + Discogs APIs              |
| 3    | Data Modeling & Cleaning              | Schema mapping + transformation prototypes               |
| 4    | Pipeline Refinement                   | ETL orchestration inside Databricks notebooks            |
| 5    | Prototyping                           | Functional multi-source ETL pipeline in Databricks       |
| 6    | Scaling Prototype                     | Optimized cluster config, checkpointing, and job control |

**Step 6 Outcome:**

> The unified ETL pipeline operates fully in Databricks using Spark 3.5 / Runtime 14.3 LTS, writing to raw and silver zones in Azure Blob Storage.  Step 6 was approved by mentor (10/21/25) and serves as the canonical code baseline for cloud deployment design.

------

## ☁️ Step 7 – Create the Deployment Architecture *(Current Stage)*

### 🎯 Purpose

This step translates the functional ETL pipeline into a **cloud-deployable architecture**, complete with infrastructure definitions and supporting documentation.  It bridges design and execution — demonstrating how each data engineering component maps to Azure resources.

### 🧱 Active Deliverables

| File                                                       | Purpose                                                      |
| ---------------------------------------------------------- | ------------------------------------------------------------ |
| `/architecture/diagrams/Step7_Architecture_Diagram.drawio` | Finalized architecture diagram (annotated, color-coded)      |
| `/doc/Step7_Architecture_Diagram.png`                      | A high-resolution PNG export of draw.io diagram              |
| `/docs/step07_architecture.md`                             | Narrative describing component roles and design rationale    |
| `/infrastructure/`                                         | Infrastructure-as-Code (ARM JSON templates + naming conventions) |

### 🧰 Current Development Environment

- **Host:** Windows 10 Home (local development)
- **Environment:** PowerShell with active Python virtual environment (venv)
- **IDE:** Visual Studio Code (launched from within venv)
- **Version Control:** Git (branch: `step7-dev`)
- **Cloud Context:** Azure Resource Manager (template-level only, no live deployments yet)

*Note:* Earlier pipeline work (Steps 4–6) occurred in **Azure Databricks** within the cloud workspace. Step 7 returns to a **local development environment** to construct and validate the infrastructure scaffolding before testing deployments in Step 8.

### 🧩 Development Workflow

1. Activate Python venv in PowerShell and launch VS Code

   ```powershell
   .venv\Scripts\Activate; code .
   ```

   

3. Run the IaC script generator:

   ```powershell
   python create_arms.py
   ```

4. Validate creation of `/infrastructure/` templates.

5. Commit and push to `step7-dev` branch:

   ```powershell
   git add infrastructure/
   git commit -m "Step 7: add IaC scaffolding (ARM skeletons)"
   git push origin step7-dev
   ```

### 🧭 Diagram Overview

The Step 7 architecture defines a modular, cloud-scalable layout:

- **Ingestion:** TMDB and Discogs API extractors.
- **Storage:** Azure Blob Storage (raw/silver/gold zones).
- **Processing:** Azure Databricks workspace executing ETL notebooks.
- **Security:** Azure Key Vault for API keys and credentials (via Managed Identity).
- **Orchestration:** Databricks orchestration.
- **Monitoring:** Azure Monitor + Log Analytics.
- **Serving:** Power BI for analytics and visualization.

Each component is represented in both the diagram and corresponding ARM template skeleton.

### 🧾 Supporting Documents

| File                                   | Description                                                  |
| -------------------------------------- | ------------------------------------------------------------ |
| `docs/step07_architecture.md`          | 3–4 sentence summary and rationale of architectural choices. |
| `infrastructure/naming_conventions.md` | Standardized resource naming guide across Azure assets.      |

------

## 🧠 Development Modes Recap

| Mode                 | Description                  | Typical Usage                                      |
| -------------------- | ---------------------------- | -------------------------------------------------- |
| **Local (Windows)**  | VS Code in PowerShell venv   | For IaC creation, doc editing, and version control |
| **Local (Ubuntu)**   | VS Code + Python venv in WSL | For Spark job prototyping (alternate dev path)     |
| **Databricks Cloud** | Notebook-based Spark jobs    | For pipeline execution, scaling, and testing       |

Step 7 occurs entirely in **Local (Windows)** mode.  Steps 8–11 will reintroduce the **Databricks Cloud** and Azure-native tools for testing and final deployment.

------

## 🚀 Next Step – Step 8: Deploy Code for Testing *(Upcoming)*

- Deploy ARM templates from `/infrastructure/` to create a dedicated test resource group.
- Validate Databricks job linkage and Key Vault access policies.
- Execute sample pipeline run against test data.
- Document testing environment in `docs/step08_testing_environment.md`.

------

## 📘 Repository Structure

```
project-root/
├── architecture/
│ ├── diagrams/
│ │ ├── step7_architecture_draft.drawio
│ │ └── step7_architecture_draft.png
│ └── notes_architecture_decisions.md
│
├── archive/ # Legacy / retired scripts
│ ├── OLD_step_01_acquire_discogs.py
│ ├── OLD_step_02_fetch_tmdb.py
│ └── step_04_legacy_match_discogs_tmdb.py
│
├── data/
│ ├── intermediate/ # Local transformation outputs
│ ├── metrics/ # Validation metrics & visuals
│ └── validation/ # Schema comparison CSVs
│
├── docs/
│ ├── 01_Unguided_Capstone_Runbook.md
│ ├── 02_Mentor Submission & Cleanup Workflow.md
│ ├── GPT Anchors Log.md
│ ├── README_TODO.md
│ └── step07_architecture.md
│
├── evidence/
│ └── Azure main.bicep Orchestrator What-If Output.png
│
├── infrastructure/
│ ├── databricks.bicep
│ ├── functionapp.bicep
│ ├── keyvault.bicep
│ ├── main.bicep
│ ├── monitoring.bicep
│ ├── storage_account.bicep
│ ├── vnet.bicep
│ ├── naming_conventions.md
│ └── storage_account.json # legacy ARM stub (pre-Bicep)
│
├── logs/
│ ├── cleanup.log
│ ├── pipeline.log
│ └── validation/validation.log
│
├── notebooks/
│ ├── ScratchPad.py.ipynb
│ └── Unguided Capstone – Step 6 Databricks with ADLS Integration.ipynb
│
├── scripts/
│ ├── step_01_acquire_tmdb.py
│ ├── step_02_query_discogs.py
│ ├── step_03_prepare_tmdb_input.py
│ ├── step_04_validate_schema_alignment.py
│ ├── step_05_match_and_enrich.py
│ ├── step_06_scale_prototype.py
│ ├── QA/ # Quality-assurance utilities
│ └── utils.py, utils_schema.py
│
├── scripts_spark/ # Spark extract prototypes
│ ├── extract_spark_tmdb.py
│ └── extract_spark_discogs.py
│
├── slides/
│ ├── Step_6_Slide_Deck_Updated.pptx
│ └── Unguided Capstone Remaining Slides.md
│
├── config.json
├── pyproject.toml
├── requirements.txt
└── README.md
```



---

## 📑 Evidence for Step 7 Validation

To confirm the infrastructure-as-code design is valid yet cost-neutral, all Bicep templates were verified using **Azure CLI “what-if” simulations**.  
These previews confirmed Azure recognizes each resource definition, dependency, and parameter without performing live provisioning.

**Validation Summary**

| Template                     | Validation Command                                           | Result      |
| ---------------------------- | ------------------------------------------------------------ | ----------- |
| `storage_account.bicep`      | `az deployment group what-if --template-file infrastructure/storage_account.bicep` | ✅ Passed    |
| `vnet.bicep`                 | `az deployment group what-if --template-file infrastructure/vnet.bicep` | ✅ Passed    |
| `keyvault.bicep`             | `az deployment group what-if --template-file infrastructure/keyvault.bicep` | ✅ Passed    |
| `databricks.bicep`           | `az deployment group what-if --template-file infrastructure/databricks.bicep` | ✅ Passed    |
| `functionapp.bicep`          | `az deployment group what-if --template-file infrastructure/functionapp.bicep` | ✅ Passed    |
| `monitoring.bicep`           | `az deployment group what-if --template-file infrastructure/monitoring.bicep` | ✅ Passed    |
| `main.bicep` (orchestration) | `az deployment group what-if --template-file infrastructure/main.bicep` | ✅ All Green |

> <img src="assets/Azure main.bicep Orchestrator What-If Output.png" alt="Azure main.bicep Orchestrator What-If Output" style="zoom:80%;" />

**Interpretation:**  
All six modules and the main orchestration layer are schema-compliant and ready for controlled deployment in Step 8. No Azure resources were actually provisioned; hence, no costs incurred.

---

## 🧾 License & Credits

This project is authored by **M. Holahan** as part of the **Springboard Data Engineering Bootcamp** capstone series.  External APIs used include [TMDB](https://developer.themoviedb.org/) and [Discogs](https://www.discogs.com/developers/).

Mentor: Akhil — Step 6 approved on 2025-10-21.
 Current sprint: *Paused phase – Step 7 (Architecture & IaC Buildout)* through November 3, 2025.

------

**Status:** 🟩 Active (Step 7 – Deployment Architecture)

**Branch:** `step7-dev`
 **Next Milestone:** Step 7 submission freeze → Step 8 testing deployment setup.

------

### 📄 Repository Metadata

- **Last Updated:** October 23, 2025
- **Active Branch:** `step7-dev`
- **Next Milestone:** Step 7 submission freeze → Step 8 testing deployment setup
- **Primary Author:** M. Holahan
- **Repository URL:** [GitHub – mtholahan/unguided-capstone-project](https://github.com/mtholahan/unguided-capstone-project)

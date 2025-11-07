# Unguided Capstone – TMDB + Discogs Data Pipeline

**Version 3.1.0  |  Step 9 – Deploy Production Code & Process Dataset  |  Status:** 🟩 Stable  |  Branch: `step9-submission`

**Mentor:** Akhil

------

## 🧭 Context Recap

Building upon Step 8’s successful test deployment, this phase represents the **production promotion** of the Medallion architecture. All test-validated components were reconfigured under new production-grade compute and storage environments. The pipeline now processes the complete dataset at scale, leveraging Azure-managed orchestration, logging, and lineage tracking.

------

## 🎯 Project Overview

This release delivers the **production deployment** of the TMDB + Discogs Medallion data pipeline.
 The pipeline executes the **Bronze → Silver → Gold** data flow under **Azure Databricks Runtime 16 LTS**, utilizing **Azure Data Lake Storage Gen2** for persistence and **PySpark 3.5** for distributed compute.

Code validated in Step 8 was promoted to production without modification to business logic, ensuring reproducibility. Execution metrics confirm **1,709 strong matches**, schema alignment across all layers, and verified lineage through JSON logs.

------

## 📚 Data Sources

- **TMDB API v3:** Movie metadata
- **Discogs API:** Artist and record release catalog

Combined, these sources enable multi-domain analytics linking film and music metadata. During production runs, data ingestion handled full API pagination and adaptive rate limiting to prevent throttling.

------

## ⚙️ Production Objectives

- Deploy finalized PySpark ETL to Azure Databricks cluster at scale
- Persist outputs to **Azure Data Lake Gold** container in `.parquet` format
- Validate lineage, schema, and runtime metrics through automated JSON audit logs
- Document architecture, runtime, and deployment topology per rubric requirements

------

## 🏗️ Production Architecture (Updated)

The architecture remains consistent with Step 7, incorporating optimized cluster sizing and Azure cost controls.)

![ungcap_architecture_step9](assets/ungcap_architecture_step9-1762499398410-9.png)

> [!NOTE]
>
> The production configuration preserves the logical topology defined in Step 7 but introduces modular Bicep definitions, Databricks Runtime 16 LTS, and integration with **Azure Monitor + Log Analytics**. These updates improve observability, maintainability, and cost governance.

### **Key Components**

| Layer          | Azure Service                  | Purpose                          |
| -------------- | ------------------------------ | -------------------------------- |
| **Bronze**     | ADLS Container `raw/`          | Raw TMDB + Discogs ingestion     |
| **Silver**     | ADLS Container `intermediate/` | Cleaned and standardized records |
| **Gold**       | ADLS Container `gold/`         | Matched, enriched outputs        |
| **Compute**    | Databricks Cluster             | PySpark execution at scale       |
| **Monitoring** | Azure Log Analytics            | Step 10 dashboard foundation     |

### Azure Databricks Workspace

![databricks_workspace_overview](assets/databricks_workspace_overview-1762499474363-11.png)

### Azure Resources

![azure_resource_groups](assets/azure_resource_groups-1762499491900-13.png)

### 📘 **Azure Resource Organization**

| Resource Group                    | Purpose                       | Key Resources                             |
| --------------------------------- | ----------------------------- | ----------------------------------------- |
| **`rg-unguidedcapstone`**         | Core production workspace     | `ungcap-dbws`, `ungcap-kv`, `ungcap-vnet` |
| **`rg-unguidedcapstone-test`**    | Step 9 validation environment | `ungcapstor01`, `ungcapkv01`              |
| **`rg-unguidedcapstone-managed`** | Databricks-managed compute    | Managed by Azure                          |
| **`NetworkWatcherRG`**            | Monitoring workspace          | Diagnostic use only                       |
| **`capstone-databricks-managed`** | Legacy prototype group        | Archived                                  |

> [!NOTE]
> Production workloads execute entirely in `rg-unguidedcapstone`, using managed identities for secure cross-RG access to storage and Key Vault resources.

------

## 🚀 Execution Procedure

1. Attach to production cluster (`capstone-prod-cluster`).
2. Configure parameters as appropriate with `config.py`
3. Execute `Pipeline_Runner.ipynb` to process complete TMDB + Discogs dataset.
4. Validate Gold-layer outputs in `wasbs://gold@<storage>.blob.core.windows.net/`.
5. Confirm lineage and runtime logs in `/data/metrics/`.

### Production Run Highlight Log

![data_pipeline_curated_production_log](assets/data_pipeline_curated_production_log-1762499636981-15.png)

------

## 📊 Pipeline Execution Metrics

| Metric                      | Value                                       |
| --------------------------- | ------------------------------------------- |
| **Total Processed Records** | 39,718 (10,000 TMDB + 29,718 Discogs)       |
| **Strong Matches**          | 1,709                                       |
| **Duration (min)**          | 26:23                                       |
| **Cluster Type**            | Standard Databricks 16 LTS (2-node)         |
| **Cost Optimization**       | Auto-terminate, spot VMs, ephemeral compute |

### Medallion Lineage Summary

| Step               | Layer  | Records Out | Duration (sec) | Output                  |
| ------------------ | ------ | ----------- | -------------- | ----------------------- |
| Extract TMDB       | Bronze | 10,000      | 288            | raw/tmdb                |
| Extract Discogs    | Bronze | 29,718      | 532            | raw/discogs             |
| Prepare Candidates | Silver | 3,605       | 84             | intermediate/candidates |
| Match & Enrich     | Gold   | 1,709       | <1             | gold/matches            |

> **Total Match Rate:** 47.4 %
>  **Run ID:** `20251107T023645`

------

## 💰 Cost Optimization & Resource Management

Production clusters are ephemeral by design — automatically terminated post-run.
Azure cost analysis shows 78% cost reduction through use of **Standard_DS3_v2** node class, short-lived job clusters, and active resource cleanup post-deployment.

------

## 📂 Repository Structure (Step 9 – Production Deployment)

```
unguided-capstone-project/
├── README.md
├── _databricks.yml
├── architecture/
│ └── diagrams/
├── assets/
│ └── Azure main.bicep Orchestrator What-If Output.png
├── config.json
├── data/
│ ├── cache/
│ ├── intermediate/
│ ├── logs/
│ ├── metrics/
│ ├── mock/
│ ├── processed/
│ ├── raw/
│ └── validation/
├── evidence/
│ └── Azure main.bicep Orchestrator What-If Output.png
├── infrastructure/
│ ├── databricks.bicep
│ ├── functionapp.bicep
│ ├── keyvault.bicep
│ ├── main.bicep
│ ├── monitoring.bicep
│ ├── naming_conventions.md
│ ├── storage_account.bicep
│ ├── ungcap-step8-test.json
│ └── vnet.bicep
├── logs/
│ ├── cleanup.log
│ ├── pipeline.log
│ └── validation/
├── notebooks/
│ ├── Data_Inspection_Notebook.ipynb
│ ├── Pipeline_Runner_Notebook.ipynb
│ └── Testing_Notebook.ipynb
├── pyproject.toml
├── rebuild_venv.sh
├── requirements_cluster.txt
├── requirements_locked.txt
├── requirements_stable.txt
├── scripts/
│ ├── init.py
│ ├── pycache/
│ ├── base_step.py
│ ├── bootstrap.py
│ ├── config.py
│ ├── extract_spark_discogs.py
│ ├── extract_spark_tmdb.py
│ ├── inventory_pipeline_outputs.py
│ ├── main.py
│ ├── match_and_enrich.py
│ ├── prepare_tmdb_discogs_candidates.py
│ ├── tests/
│ ├── utils.py
│ ├── utils_schema.py
│ └── validate_schema_alignment.py
├── slides/
│ └── Step_8_Slide_Deck.pptx
└── tests/
├── abfss:/
├── conftest.py
├── test_pipeline_config.py
├── test_report.txt
└── test_spark_session.py
```

------

## 🖼️ Slide Deck Integration

[View Slide Deck → Step9_Presentation.pptx](slides/Step9_Presentation.pptx)

This presentation summarizes:

- Migration from Step 8 test cluster to production
- Finalized architecture and environment configuration
- Runtime performance highlights and lineage proofs
- Gold-layer schema validation and sample outputs


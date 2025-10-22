# GPT Anchors Log

A living record of handoff anchors between ChatGPT sessions.  
Anchors capture context, current milestone, next action, and dependencies for seamless continuity across chats.

---

## Template

```
Anchor this as chat [ANCHOR_NAME] with next action + dependencies. Use this template:
Resume from anchor: [ANCHOR_NAME]
Context: [3–4 lines on project/sprint state]
Current milestone: [what’s done]
Next action: [one concrete step]
Dependencies: [keys/env/tools/people]
```



**11:48 10/22/2025**

Resume from anchor: [UnguidedCapstone_TMDB_Refactor02_Step_07_In_Flight]

**Context:** You’re mid-sprint on **Unguided Capstone Step 7 (Create the Deployment Architecture)**. The architecture diagram and narrative are complete, and local IaC scaffolding is being built in your Windows 10 + PowerShell + VS Code venv environment. This phase focuses on codifying your Azure design into reproducible ARM templates before freeze and Step 8 deployment testing.

**Current milestone:**
 ✅ Step 7 architecture diagram finalized and exported
 ✅ Naming conventions defined
 ✅ `create_arms.py` completed
 ⏳ Infrastructure templates pending commit to `step7-dev`

**Next action:**
 Run `python create_arms.py` in your PowerShell venv to generate and verify the 5 ARM template skeletons, then commit and push them to `step7-dev`.

**Dependencies:**

- **Keys/Env:** Active Python virtual environment (venv)
- **Tools:** VS Code, PowerShell, Git, Azure Resource Manager schema
- **People:** None (mentor decoupled until post–Nov 3)



**19:45 10/20/2025**

Resume from anchor: [ANCHOR_NAUnguidedCapstone_TMDB_Refactor02_Step_06_Submission_De-debug]

Context:
 We’re stabilizing Step 6 (“Scale Your Prototype”) of the Unguided Capstone after migrating TMDB and Discogs extraction scripts to PySpark for Databricks + ADLS Gen2. Both jobs now run through the cluster, but output validation shows malformed title parsing and incorrect ADLS URIs traced to legacy config logic. Environment, secrets, and managed-identity access are verified.

Current milestone:
 ✅ Spark runtime confirmed (Databricks 14.3 LTS, Connect off)
 ✅ Managed Identity + ADLS Gen2 external location verified
 ✅ Both PySpark scripts execute end-to-end
 🚧 Data write and title handling bugs identified in `extract_spark_tmdb.py` and `extract_spark_discogs.py`

Next action:
 🔧 Patch both extract scripts to (a) coerce `GOLDEN_TITLES_TEST` into a list of titles instead of characters, and (b) lock `container_uri` and `tmdb_path` to the correct ADLS Gen2 URIs (`markcapstoneadls`). Retest Databricks execution and validate Parquet outputs in `/raw/tmdb/` and `/raw/discogs/`.

Dependencies:

- Repo: `unguided-capstone-project/scripts_spark/`
- Environment: Databricks cluster `capstone-blob-cluster` (Runtime 14.3 LTS)
- Storage: `markcapstoneadls` (container `raw`)
- Secrets: `capstone-secrets` scope (`tmdb_api_key`, `discogs_api_key`)
- Mentor (Akhil) for final Step 6 notebook review and rubric sign-off





**12:04 10/20/2025**

**Resume from anchor:**
 `[ANCHOR_NAUnguidedCapstone_TMDB_Refactor02_Step_06_Submission_Debug]`

**Context:**
 Working on Step 6 (“Scale Your Prototype”) of the Unguided Capstone.
 TMDB extraction runs successfully in Databricks but output verification fails — writes to local DBFS instead of Azure Blob.
 We’ve built a new cluster (`capstone-blob-cluster`, Spark 3.5.0, Scala 2.12) and validated Azure secret scope `markscope`.
 Current focus: achieving a working WASBS-based write to the Blob container (`raw@markcapstonestorage.blob.core.windows.net`).

**Current milestone:**
 ✅  Verified Spark environment on Databricks runtime 14.3 LTS
 ✅  Confirmed secret retrieval from `markscope`
 ✅  Validated ABFS + mount configs fail under current cluster policy
 🚧  Debugging direct WASBS connector write (SharedKey authentication)

**Next action:**
 Test direct WASBS write using this working minimal cell:

```
spark.conf.set(
    "fs.azure.account.key.markcapstonestorage.blob.core.windows.net",
    dbutils.secrets.get("markscope","azure-storage-key")
)
test_path = "wasbs://raw@markcapstonestorage.blob.core.windows.net/test_write_simple/"
df = spark.createDataFrame([(1,"ok")], ["id","status"])
df.write.mode("overwrite").parquet(test_path)
```

If successful, refactor `extract_spark_tmdb` and `extract_spark_discogs` to write to
 `wasbs://raw@markcapstonestorage.blob.core.windows.net/tmdb/` and `/discogs/`.

**Dependencies:**

- Azure Storage Account `markcapstonestorage` (container `raw`)
- Databricks Secret Scope `markscope` (key `azure-storage-key`)
- Cluster `capstone-blob-cluster` (Runtime 14.3 LTS / Spark 3.5.0)
- dbutils + PySpark 3.5 environment
- No external mentor or reviewer yet engaged for this stage



**00:20 10/19/2025**

### 🧭 **Resume from anchor:**

**[UnguidedCapstone_TMDB_Refactor02_Step_06_Submission_Prep]**

------

**Context:**
 You’ve successfully scaled both TMDB and Discogs metadata extracts into Spark using Databricks (Step 6 refactor). The pipeline runs cleanly end-to-end, writing Parquet outputs to Azure Blob under `/raw/tmdb/` and `/raw/discogs/`. Environment variables and Blob keys are verified. You’re now preparing the project for mentor submission and review.

------

**Current milestone:**
 ✅ *Unguided Capstone Step 6 (Scale Your Prototype)* functional completion achieved

- `extract_spark_tmdb.py` and `extract_spark_discogs.py` validated
- Pipeline Preflight notebook executes cleanly on Databricks cluster
- Blob persistence confirmed via Parquet files
- Environment + cluster configuration stable

------

**Next action:**
 🧾 **Produce mentor-ready submission assets for Step 6:**

- Add a short **Step 6 section to README.md** (run instructions + prerequisites)
- Prepare a submission-ready *Pipeline Preflight* notebook (purpose, sequence, and expected outputs) for mentor use
- Capture 1–2 screenshots (green check notebook run + Blob file listing) → save to `/evidence/step6/`

------

**Dependencies:**

- 🧠 Current Databricks cluster (capstone-cluster, Spark 3.5+)
- 🗝️ Environment variables: `TMDB_API_KEY`, `DISCOGS_API_KEY`
- 🔑 Azure Blob key in Spark config
- 📁 Repo path: `/Workspace/Repos/markholahan@pm.me/unguided-capstone-project`
- 👤 Mentor Akhil (for final review and rubric confirmation)



**16:21 10/18/2025**

**Resume from anchor:** [UnguidedCapstone_TMDB_Refactor02_Step_06_Databricks_Working]

**Context:**
 The TMDB → Spark extraction workflow is now operational in Databricks Repos.
 Environment fallback for TMDB API key confirmed functional.
 Azure Blob connection validated via `abfss://` Parquet writes and successful Spark jobs.

**Current milestone:**
 ✔️ Step 01 (TMDB Spark Extract) fully refactored, executed, and persisted to Blob storage (`/raw/tmdb`).

**Next action:**
 Proceed to implement **Step 02 (Discogs Spark Extract)** — mirror TMDB Spark pattern using the same `BaseStep` structure, ensuring similar environment key retrieval and Blob output configuration.

**Dependencies:**

- ✅ Active TMDB API key (env var: `TMDB_API_KEY`)
- ✅ Azure Blob storage access key (via `spark.conf.set`)
- 🧰 Databricks Repos environment with Spark 3.5+
- 📁 Shared repo path: `/Workspace/Repos/markholahan@pm.me/unguided-capstone-project`



**13:29 10/18/2025**

Resume from anchor: [UnguidedCapstone_TMDB_Refactor02_Step_06_Databricks_More_Almost]

Context:
 The TMDB–Discogs Unguided Capstone is now fully integrated with the Azure Databricks workspace and validated OAuth connection to Blob Storage. The environment sync pipeline (VS Code ↔ Ubuntu ↔ Databricks) remains stable through `rebuild_venv.sh`. GitHub will remain the sole repo for code versioning, avoiding Azure DevOps complexity.

Current milestone:
 ✅ Verified Databricks workspace access and tested manual Spark session initialization
 ✅ Confirmed service principal-based OAuth authentication works with Blob container
 ✅ Locked Python environment (`requirements_locked.txt`) now syncs across shells

Next action:
 → Connect GitHub repository directly to Databricks workspace and pull the current `unguided-capstone-project` codebase to validate notebook-based Spark I/O (read/write via OAuth).

Dependencies:

- Azure Databricks (Premium, East US) workspace
- GitHub repo: `unguided-capstone-project`
- Azure Storage: `markcapstonestorage / capstone-data`
- OAuth secrets (client-id, client-secret) stored in Databricks scope: `capstone-secre`

**00:47 10/18/2025**

Resume from anchor: [UnguidedCapstone_TMDB_Refactor02_Step_06_Databricks_Almost]

**Context:**
 The TMDB–Discogs Unguided Capstone has transitioned to a fully integrated, multi-shell development ecosystem (VS Code, Git Bash, Ubuntu) sharing a unified virtual environment (`~/pyspark_venv311`). The Databricks workspace and Azure Blob OAuth connection are verified. Environment management is now automated through `rebuild_venv.sh`, producing synced `requirements_stable.txt` and `requirements_locked.txt`.

**Current milestone:**
 ✅ Verified end-to-end environment consistency (VS Code ↔ Git Bash ↔ Ubuntu ↔ Databricks)
 ✅ Clean dependency architecture diagram finalized
 ✅ `rebuild_venv.sh` integrated with locked requirements generation

**Next action:**
 → Execute Spark cloud I/O validation on Databricks by reading a small dataset from Azure Blob Storage and writing processed results to `/output/` via Databricks notebook. Confirm end-to-end OAuth access and data persistence.

**Dependencies:**

- Environment: Azure Databricks (Premium, East US)
- Storage: `markcapstonestorage` / container `capstone-data`
- Secrets scope: `capstone-secrets` (`client-id`, `client-secret`)
- Tenant ID: from registered service principal
- Cluster: auto-termination ≤ 15 min, attached to workspace



**23:33 10/16/2025**

**Resume from anchor:** [UnguidedCapstone_TMDB_Refactor02_Step_06_Databricks]

**Context:**
 The unguided TMDB–Discogs capstone successfully transitioned from local PySpark to a fully authenticated Azure Databricks + Blob Storage environment using OAuth and Databricks secrets.

**Current milestone:**
 ✅ Databricks workspace provisioned
 ✅ Azure Blob OAuth access verified (`abfss://capstone-data@markcapstonestorage...`)
 ✅ Service principal + secret scope fully operational

**Next action:**
 → Validate Spark cloud I/O by reading a sample dataset from Blob and writing results to `/output/` via Databricks notebook.

**Dependencies:**
 Environment: Azure Databricks (Premium, East US)
 Storage: `markcapstonestorage / capstone-data`
 Secrets scope: `capstone-secrets` (`client-id`, `client-secret`)
 Tenant ID: from service principal
 Cluster: attach one with Auto-Termination ≤ 15 min



**17:00 10/16/2025**

Resume from anchor: **[UnguidedCapstone_TMDB_Refactor02_Step_06_It_Stopped_Raining]**

**Context:** The unguided TMDB–Discogs capstone now runs fully under Ubuntu (WSL2) with a stable PySpark 3.5 / Pandas 2.0 environment. The `_new_Index` serialization bug is resolved, and Step 06 successfully executes end-to-end.

**Current milestone:**
 ✅ PySpark pipeline stable and verified locally
 ✅ `rebuild_venv.sh` finalized (with `--force` option)
 ✅ `README.md` refactored with reproducibility and environment lifecycle
 ✅ Windows PowerShell scripts deprecated

**Next action:**
 → Prepare Step 07 Azure deployment by validating **Blob Storage connectivity** and confirming that `requirements_stable.txt` installs cleanly on a new Azure compute instance or HDInsight/Databricks cluster.

**Dependencies:**

- Environment: `~/pyspark_venv311` active
- Tools: Azure CLI ≥ 2.60, `requirements_stable.txt` present
- Credentials: Azure Storage account + container write access
- Scripts: `step_06_scale_prototype.py` (baseline for cloud submission)
- Optional support: Mentor Akhil (for Azure configuration or resource quota)



**12:57 10/16/2025**

**Resume from anchor:** [UnguidedCapstone_TMDB_Refactor02_Step_06_Ubuntu_Unending]

**Context:** Debugging and stabilizing the PySpark pipeline in Ubuntu WSL2 for the unguided TMDB capstone project. Pandas–Spark UDF compatibility and serialization issues surfaced after migration to Python 3.12 / PySpark 4.x.

**Current milestone:**
 ✅ Environment operational in VS Code (WSL: Ubuntu)
 ✅ Virtualenv `pyspark_venv312` set up
 ✅ All core packages installed (PySpark 4.0.1, Pandas 2.1+, Matplotlib, RapidFuzz)
 ✅ `_new_Index` unpickling fix integrated (pending runtime verification)

**Next action:**
 Run a **local serialization sanity test** to confirm that the `_new_Index` patch successfully prevents the PickleException before rerunning the full Step 06 Spark job.

**Dependencies:**

- Environment: `pyspark_venv312` active in VS Code (WSL: Ubuntu)
- Tools: Python 3.12 +, PySpark 4.0.1, Pandas 2.1.x
- Script: `step_06_scale_prototype.py` (contains patch + UDF)
- Verification: small Spark session + sample Pandas object test



**19:36 10/15/2025**

**Resume from anchor:** [UnguidedCapstone_TMDB_Refactor02_Step_06_Ubuntu]
 **Context:** Transitioned PySpark prototype from unstable Windows execution to WSL2 Ubuntu for stable UDF and RapidFuzz operations in Step 06 (scaling prototype).
 **Current milestone:** Spark + Python environment verified inside WSL2; data files and script structure confirmed; Java 17 aligned; ready for execution in Linux context.
 **Next action:** Run `python scripts/step_06_scale_prototype.py` inside the WSL2 Ubuntu virtual environment using Spark’s `local[*]` mode and confirm end-to-end data flow through Step 06A (matching + metrics output).
 **Dependencies:**

- Environment: WSL2 Ubuntu, Python venv with `pyspark`, `rapidfuzz`, `pandas`, `pyarrow`
- Config: Java 17 (Temurin), Spark set to `local[*]`
- Files: `/mnt/c/Projects/unguided-capstone-project/data/intermediate/tmdb_discogs_candidates_extended.csv`
- Tools: SparkSession (PySpark), RapidFuzz
- People: None (solo dev checkpoint)



**17:36 10/15/2025**

Resume from anchor: **[UnguidedCapstone_TMDB_Refactor02_Step_06]**

**Context:** Transitioning from TMDB→Discogs refactor (Step 05 Phase 2 Rescue Plan) into PySpark refactor phase for scalable matching validation.

**Current milestone:** Step 05 complete and validated; normalization stable; histogram and metrics verified. Java environment setup script (`setup_java_env.ps1`) ready and tested; Temurin 11 JDK installation pending to enable PySpark gateway.

**Next action:** Install **Temurin 11 LTS (HotSpot)** → confirm via
 `java -version` → rerun

```
powershell -ExecutionPolicy Bypass -File scripts/setup_java_env.ps1
```

to auto-set `JAVA_HOME` and validate Spark startup.

**Dependencies:**
 ✅ Python 3.x venv (active)
 ⚙️ Temurin 11 LTS JDK
 📦 PySpark 3.x
 📁 `C:\Projects\unguided-capstone-project\scripts`
 🧠 No mentor dependency (independent phase)



**03:29 10/15/2025**

Resume from anchor: [UnguidedCapstone_TMDB_Refactor01_Step_05]

Context: TMDB→Discogs refactor pipeline operational through Step 05; fuzzy‐matching now runs cleanly with BaseStep integration and metrics output. Schema validation (Step 04) stable, normalization utilities consolidated in `utils.py`.

Current milestone: Step 05 executed end-to-end with 64 K candidate pairs and metrics JSON generated. Data linkage remains weak (avg score ≈ 48) due to divergent TMDB vs Discogs naming conventions and missing cross-IDs.

Next action: Implement “Phase 2 Rescue Plan” — enhance matching with year-bounded fuzzy logic (`±1 year`), partial-ratio scoring, and improved normalization; generate a score-distribution histogram (`metrics/step05_score_distribution.png`) to visualize match quality before Step 06 (PySpark scaling).

Dependencies:
 ✅ Valid .env (API tokens)
 ✅ Existing outputs from Steps 04 & 05 (`tmdb_discogs_matches.csv`, `step05_matching_metrics.json`)
 ⚙️ Libraries – pandas, rapidfuzz, matplotlib, re, python-dotenv
 🧩 Branch = `step6-dev`   |  Virtual env active   |  `utils.py` (normalization functions)



**21:53 10/14/2025**

**Resume from anchor:** [UnguidedCapstone_TMDB_Refactor01]
 **Context:** Unguided Capstone – TMDB→Discogs directional refactor (Sprint A). TMDB Step 01 acquisition and checkpoint persistence validated; Discogs Step 02 authenticated via token.
 **Current milestone:** Environment stabilized; Discogs token conflict resolved and config defensive checks added.
 **Next action:** Refactor `step_02_query_discogs.py` to use relaxed, fuzzy query logic (`"<title> soundtrack"`, no `type`/`genre` filters) and verify non-zero Discogs JSON output for sample titles (“Blade Runner”, “Amélie”, “Inception”).
 **Dependencies:**

- ✅ Valid `.env` with `DISCOGS_TOKEN` and `TMDB_API_KEY`
- ✅ `config.py` loads with `override=True`
- 🧩 Internet access to Discogs API (`https://api.discogs.com/database/search`)
- ⚙️ Tools: `requests`, `python-dotenv`, `logging`



**01:02 10/15/2025**

Resume from anchor: [**Pipeline_TMBD_to_Discogs_Refactor_Pre_Step04**]
 Context: TMDB→Discogs pipeline refactor (Sprint A) stabilized through Step 03; all three steps now share a single golden-aware title list and unified metrics flow.
 Current milestone: Steps 01–03 complete, integrated, and validated under both GOLDEN (subset) and AUTO (full) modes with correct persistence, checkpointing, and rollup metrics.
 Next action: Implement **Step 04 – Harmonized Data Validation & Schema Alignment**, ensuring normalized column types, consistent ID joins, and integrity checks between TMDB and Discogs outputs before enrichment.
 Dependencies:

- ✅ Valid `.env` with `DISCOGS_TOKEN` and `TMDB_API_KEY`
- ✅ Existing outputs: `titles_to_process.json`, `tmdb_raw/`, `discogs_raw/`, `tmdb_discogs_candidates_extended.csv`
- ⚙️ Tools: `pandas`, `pyarrow`, `python-dotenv`, `logging`
- 🧩 Branch = `step6-dev`; ensure virtual environment active



**Tues, 10/14/15: 12:25 PM**

Resume from anchor: PACKAGE_IMPORT_FIX_V1
Context: Unguided Capstone – TMDB→Discogs directional refactor (Sprint A).
Current milestone: All intra-package imports normalized using `from scripts.<module>` syntax; package runs clean via `python -m scripts.step_01_acquire_tmdb`.
Next action: Run TMDB acquisition step to verify JSON output in `data/raw/tmdb_raw/`. 
If successful, proceed to scaffold Step 2 (`step_02_query_discogs.py`) using TMDB titles as input.
Dependencies: Valid TMDB API key loaded via setup_env.ps1; Python v3.10+ environment.

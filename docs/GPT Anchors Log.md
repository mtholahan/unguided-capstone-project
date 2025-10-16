# GPT Anchors Log

A living record of handoff anchors between ChatGPT sessions.  
Anchors capture context, current milestone, next action, and dependencies for seamless continuity across chats.

---

## Template

```
Anchor this as chat [ANCHOR_NAME] with next action + dependencies. Use this template:
Resume from anchor: [ANCHOR_NAME]
Context: [1–2 lines on project/sprint state]
Current milestone: [what’s done]
Next action: [one concrete step]
Dependencies: [keys/env/tools/people]
```



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

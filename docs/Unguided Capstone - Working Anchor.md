# 🧭 Unguided Capstone – Working Anchor  
**Project:** MusicBrainz × TMDb Integration (Unguided Capstone)  
**Last Updated:** October 7, 2025  

---

## 🎯 Current Focus
**Milestone:** Step 5 – Local ETL Prototype (Steps 00–06)  
**Objective:** Deliver mentor-ready local ETL pipeline with automation, OOP design, and logging.  
**Status:** ✅ Ready for mentor submission (`step5-submission` branch pushed to GitHub).  
**Next Action:** Await feedback from Akhil → resume Step 6 development on new branch (`step6-dev`).  

---

## 🧩 Functional Overview

| Component                 | Status     | Notes                                                        |
| ------------------------- | ---------- | ------------------------------------------------------------ |
| Environment               | ✅ Stable   | Local `.venv` configured; PowerShell + VS Code synced.       |
| Dependency Mgmt           | ✅          | `pyarrow` installed for Parquet; Azure libraries conditionally imported. |
| Pipeline Orchestration    | ✅          | `main.py` controls step sequencing and resume logic.         |
| Logging                   | ✅          | All steps log to `logs/pipeline.log` (rotating file handler). |
| Data Acquisition          | ✅          | `step_00_acquire_musicbrainz.py` automates TSV download and audit. |
| Data Cleansing            | ✅          | `step_01_audit_raw.py`, `step_02_cleanse_tsv.py` normalize fields and schema. |
| GUID Rehydration          | ✅          | `step_03b_rehydrate_guids.py` ensures join integrity for missing identifiers. |
| Join Logic                | ✅          | `step_04_mb_full_join.py` performs full joins and structure enforcement. |
| Filtering / Enhancement   | ✅          | `step_05_filter_soundtracks_enhanced.py` refines candidate soundtracks. |
| TMDb Enrichment (Initial) | ✅          | `step_06_fetch_tmdb.py` performs limited metadata fetches.   |
| Matching / Scoring        | ⏸️ Paused   | `step_08_match_instrumented.py` validated (≈59% match accuracy). |
| Rescue Logic / Post-Match | ⏸️ Paused   | `step_09_apply_rescues.py` prepared for later integration.   |
| Coverage & Audit          | ⏸️ Deferred | `step_10b_coverage_audit.py` not yet active.                 |
| Cloud Readiness           | ⏸️ Deferred | Azure/Spark integration postponed pending Step 5 review.     |

---

## 📂 Project Directory Structure

C:\Projects\unguided-capstone-project
│
├── Scripts
│ ├── base_step.py
│ ├── config.py
│ ├── main.py
│ ├── step_00_acquire_musicbrainz.py
│ ├── step_01_audit_raw.py
│ ├── step_02_cleanse_tsv.py
│ ├── step_03b_rehydrate_guids.py
│ ├── step_03_util_check_tsv_structure.py
│ ├── step_04_mb_full_join.py
│ ├── step_05_filter_soundtracks_enhanced.py
│ ├── step_06_fetch_tmdb.py
│ ├── step_07_prepare_tmdb_input.py
│ ├── step_08_match_instrumented.py
│ ├── step_09_apply_rescues.py
│ ├── step_10b_coverage_audit.py
│ ├── step_10_enrich_tmdb.py
│ ├── step_99_ScratchPad.py
│ ├── utils.py
│ └── init.py
│
├── logs
├── outputs
├── archive\ # older/retired versions of Steps 08–10
└── docs\ # mentor documentation (workflow, changelog, quick_start)



---

## 🧱 Development Notes (as of Oct 7)

- `safe_overwrite()` implemented in `base_step.py` for atomic file writes.
- Environment confirmed idempotent between VS Code and PowerShell.
- All scripts refactored for OOP inheritance via `BaseStep` class.
- TMDb API functions modularized for re-use in `step_06` and `step_10`.
- Logs verified to capture run metadata (start/end time, row counts, file writes).
- Current ETL output verified through Parquet export.

---

## 🪜 Current Git Branching State

| Branch             | Purpose                      | Notes                                                    |
| ------------------ | ---------------------------- | -------------------------------------------------------- |
| `main`             | Stable / last approved state | To be merged after Akhil’s Step 5 approval.              |
| `step5-submission` | Mentor review branch         | Contains final Step 5 deliverable (Steps 00–06).         |
| `step5-feedback`   | Placeholder                  | Will be created if Akhil requests changes.               |
| `step6-dev`        | Forward development branch   | Next working branch once submission confirmed.           |
| `docs-integration` | Documentation                | Holds mentor workflow, changelog, and quick start files. |

---

## 🧾 Git Submission Summary (Step 5)

**Commit message:**
Step 5 – Local ETL Prototype: automated OOP pipeline (Steps 00–06)

**Tag:**
step5-approved-2025-10-07 (pending review)

**Branch message to Akhil:**
> Branch: `step5-submission`  
> Includes: Steps 00–06 (Local ETL Prototype)  
> Logs in `/logs/`, outputs in `/outputs/`.  
> Awaiting feedback before resuming Step 6 work.

---

## 🧠 Key Insights Captured

- Branch-based mentor workflow adopted (submission → feedback → tag → next branch).
- Local pipeline is now **reproducible**, **logged**, and **self-contained**.
- `Step_XX_Scripts/` directories act as reproducible checkpoints until full modularization.
- Once Step 6 is approved, refactor begins toward `/pipeline/` package architecture.

---

## 🧩 Upcoming Focus Areas

| Next Step | Goal                                 | Notes                                                        |
| --------- | ------------------------------------ | ------------------------------------------------------------ |
| Step 6    | Scaling / Pre-Spark optimization     | Begin modularizing step classes, test larger datasets locally. |
| Step 7    | Analytical ETL / Feature Engineering | Develop intermediate aggregates, genre mapping.              |
| Step 8    | Matching Metrics Refactor            | Improve algorithmic scoring + integrate rescue logic.        |
| Step 9    | CI/CD Testing                        | Integrate validation + logging consistency checks.           |
| Step 10   | Monitoring Dashboard                 | Final documentation + observability via Power BI.            |

---

## 📈 Current Deliverable Summary

**Deliverable:** Local ETL pipeline (Steps 00–06)  
**Status:** ✅ Submission-ready  
**Environment:** `.venv` (PowerShell + VS Code)  
**Execution:**  

cd Step_05_Scripts
python main.py
Output: ../outputs/
Logs: ../logs/pipeline.log

## 🪞 Purpose of This Document

This Markdown file acts as the persistent technical anchor between development sessions and ChatGPT collaborations.
At the start of each new chat:

Upload this file.

I (GPT) will read and re-sync to your latest state.

We’ll continue development seamlessly from where you left off.
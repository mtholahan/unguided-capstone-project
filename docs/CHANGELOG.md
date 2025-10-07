## 🧾 Project Changelog – Unguided Capstone (MusicBrainz × TMDb)

_Last updated: October 6 2025_

This changelog tracks mentor submission milestones and major development checkpoints from Step 5 onward.  
Each section corresponds to a **review branch** (`stepX-submission`) and its **approval tag** (`stepX-approved-YYYY-MM-DD`).

---

## [Step 5 – Local ETL Prototype] – 2025-10-06

**Branch:** `step5-submission`  
**Tag (upon approval):** `step5-approved-2025-10-07`

### 🧩 Summary

- Completed automated OOP ETL pipeline covering **Steps 00 – 06**.  
- Functions include: data acquisition (MusicBrainz), audit & cleansing, GUID rehydration, joins, filtering, and limited TMDb enrichment.
- Logging verified (`/logs/pipeline.log`) and outputs stored in `/outputs/`.  
- Execution confirmed under local `.venv` via:

```powershell
 cd Step_05_Scripts
 python main.py
```

- Azure / Spark / cloud stages deferred pending review.

### 📨 Mentor Action

Akhil to review `step5-submission` branch for:

- Pipeline structure and readability
- Logging integrity
- Correctness of joins and filtering logic

### 🧠 Notes

- Implemented `safe_overwrite()` for idempotent writes.
- `pyarrow` verified for Parquet operations.
- Environment sync between PowerShell + VS Code confirmed.

------

## [Step 6 – Scaling and Pre-Spark Optimization] – YYYY-MM-DD

**Branch:** `step6-submission`
 **Tag (upon approval):** `step6-approved-YYYY-MM-DD`

### Summary (placeholder)

*To be updated upon completion.*
 Focus: modularization, early parallelization, and prep for Spark ingestion.

------

## [Step 7 – Analytical ETL / Feature Engineering] – YYYY-MM-DD

**Branch:** `step7-submission`
 **Tag (upon approval):** `step7-approved-YYYY-MM-DD`

### Summary (placeholder)

Focus: advanced joins, data augmentation, and analytical transformation.

------

## [Step 8 – Matching & Metrics Refactor] – YYYY-MM-DD

**Branch:** `step8-submission`
 **Tag (upon approval):** `step8-approved-YYYY-MM-DD`

### Summary (placeholder)

Focus: re-instrumenting match algorithms, performance metrics, and idempotence verification.

------

## [Step 9 – Deployment / Testing Pipeline] – YYYY-MM-DD

**Branch:** `step9-submission`
 **Tag (upon approval):** `step9-approved-YYYY-MM-DD`

### Summary (placeholder)

Focus: CI/CD integration, data validation, and automated regression testing.

------

## [Step 10 – Monitoring Dashboard & Documentation] – YYYY-MM-DD

**Branch:** `step10-submission`
 **Tag (upon approval):** `step10-approved-YYYY-MM-DD`

### Summary (placeholder)

Focus: reporting, observability, and end-to-end delivery narrative for final submission.

------

## 🪜 Branch Lifecycle Map

| Step | Submission Branch   | Feedback Branch   | Approval Tag                 |
| ---- | ------------------- | ----------------- | ---------------------------- |
| 5    | `step5-submission`  | `step5-feedback`  | `step5-approved-2025-10-07`  |
| 6    | `step6-submission`  | `step6-feedback`  | `step6-approved-YYYY-MM-DD`  |
| 7    | `step7-submission`  | `step7-feedback`  | `step7-approved-YYYY-MM-DD`  |
| 8    | `step8-submission`  | `step8-feedback`  | `step8-approved-YYYY-MM-DD`  |
| 9    | `step9-submission`  | `step9-feedback`  | `step9-approved-YYYY-MM-DD`  |
| 10   | `step10-submission` | `step10-feedback` | `step10-approved-YYYY-MM-DD` |

------

## 💡 Usage Notes

- Update this file **immediately after each submission** and **after mentor approval**.
- Keep entries concise – Akhil should be able to skim for context quickly.
- This changelog doubles as your final project audit trail for Springboard documentation.

------

🧠 **GPT Suggestion:**  
Once Step 6 is underway, add a `docs/README.md` index linking to both:
- `mentor_workflow.md` – process reference  
- `CHANGELOG.md` – version record  

That tiny index file (three lines) turns your repo’s `/docs/` folder into a polished, mentor-friendly dashboard.  


















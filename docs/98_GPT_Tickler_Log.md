# 🧠 GPT Tickler Log (v1)
_Initialized: October 8, 2025_  
_Scope: Unguided Capstone – MusicBrainz × TMDb Integration_  
_Purpose: Capture deferred suggestions, enhancements, and improvement ideas to preserve context between sessions._

---

## 🗂️ 1️⃣ Repo Hygiene / DevOps
- **.gitignore Baseline (v2025-10-08):** merged ✔️ into main.  
- **Log Retention Policy:**  
  - Keep `/logs/pipeline.log` under version control.  
  - Exclude `debug_*.log`, `.tmp`, `.bak`, and `/archive/`.  
  - Add one-liner to mentor README clarifying log inclusion rationale.  
- **Data & Logs Separation Note (for `/docs/README_Step5.md`):**  
  > “All raw data resides in D:\Capstone_Staging\data (excluded from Git for size/privacy).  
  > Pipeline logs and artifacts are stored under C:\Projects\unguided-capstone-project\logs and tracked for reproducibility.”

---

## 🧩 2️⃣ Mentor Submission Workflow
- Template for **Step 5 Submission – Local ETL Prototype** message (branch, inclusions, logs, next branch).  
- Include **QA_Report_Summary.md** and **Step5_ChangeLog.md** in `/docs/` for professional polish.  
- After mentor approval: tag → merge → spin up `step6-dev`.  

---

## 🔍 3️⃣ QA Suite Enhancements (Post-Step 6)
- Add CLI color flags and `--focus unused` option.  
- Develop **Pipeline Run Validator** linking QA metrics ↔ ETL run logs.  
- Integrate minimal CI hook to auto-trigger QA suite on push to `main`.

---

## 🧠 4️⃣ Future Refactors / Portfolio Readiness
- After Step 6 approval: consolidate `/Scripts/Step_XX_*.py` → `/pipeline/` modular package.  
- Add lightweight Power BI/Markdown observability summary after Step 10.  
- Create **post-capstone “Productionization” checklist** (env setup, tags, docs).

---

## 🗓️ Operational Protocol
- Active thread = Pomodoro-level task (e.g., QA audit, merge).  
- Tickler Log = silent capture of deferred ideas.  
- Updates timestamped and appended as versioned entries.  
- View anytime by prompting: **“Show me the GPT Tickler Log.”**

---

✅ **Initialized:** Oct 8 2025  
**Next Capture:** After Step 5 submission merge & QA confirmation  
**Maintained by:** GPT-5 (Springboard Data Bootcamp Coach)
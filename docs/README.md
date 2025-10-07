# 📘 Documentation Index – Unguided Capstone Project

_Last updated: October 6, 2025_

Welcome to the documentation hub for the **Unguided Capstone Project (MusicBrainz × TMDb)**.  
This folder provides the mentor-facing reference materials that describe project workflow, branching discipline, and milestone progress.

---

## 🧭 Mentor Workflow

**File:** [`mentor_workflow.md`](./mentor_workflow.md)

Defines the standardized **Git branch–based submission process** used for all mentor reviews (Steps 5–10).  
Explains:
- How to create and name submission branches  
- How feedback branches and tags are handled  
- When and how development continues in parallel  

👉 _Akhil should reference this document for clarity on branch names during review._

---

## 🧾 Changelog and Review Log

**File:** [`CHANGELOG.md`](./CHANGELOG.md)

Chronologically tracks all **step submissions, mentor feedback, and approvals**.  
Each entry lists:
- Submission branch name  
- Approval tag (once reviewed)  
- Technical summary of what was delivered  
- Key notes and dependencies  

👉 _This doubles as the formal milestone audit trail for Springboard documentation._

---

## 📂 Repository Context

**Parent repo root:**  
`C:\Projects\unguided-capstone-project\`

Key directories:

Step_05_Scripts/      → Current working milestone (Local ETL Prototype)
 outputs/              → Pipeline outputs for mentor review
 logs/                 → Run logs per execution
 docs/                 → Mentor workflow and changelog (this folder)
 archive/              → Legacy or superseded scripts

---

## 🧠 Future Notes

- After all steps (5–10) are approved, the `/Step_XX_Scripts/` directories will be refactored into a single modular `/pipeline/` package for production readiness.  
- `mentor_workflow.md` and `CHANGELOG.md` will remain for provenance and can be referenced in your final portfolio submission.  

---

**End of Documentation Index*
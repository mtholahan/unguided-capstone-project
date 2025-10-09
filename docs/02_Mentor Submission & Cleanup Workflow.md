# 🗭 Mentor Submission & Cleanup Workflow (Unified Guide)

*Last updated: October 8, 2025*

------

## 🌟 Purpose

This document defines how mentor submissions (to Akhil) are handled using **Git branches** — along with the **cleanup protocol** that ensures each submission is lean, reproducible, and free of transient artifacts.

The goal:

- Maintain reproducible **“snapshot” submissions** for review
- Keep development unblocked
- Guarantee branch hygiene and professional DevOps practices

------

## 🧩 Overview

Each **capstone milestone (Steps 5–10)** gets its own **dedicated branch** for mentor review.

- You: submit from a clean branch (e.g. `step5-submission`)
- Akhil: reviews the branch without interference from ongoing work
- You: continue forward development safely on the next branch (`step6-dev`)
- Once approved, tag and merge the feedback branch into `main`

------

## 🦻 Full Lifecycle Workflow

### **A) Step-by-Step Submission**

#### 1️⃣ Prepare Submission

Ensure your current code runs cleanly and logs are verified.

```bash
git checkout main
git pull
git checkout -b step5-submission
git add .
git commit -m "Step 5 submission: Local ETL Prototype (Steps 00–06)"
git push origin step5-submission
```

#### 📤 Notify Akhil:

> **Subject:** Step 5 Submission – Local ETL Prototype
>  **Branch:** step5-submission
>  **Includes:** Steps 00–06 under /Scripts
>  **Logs:** /logs/, **Outputs:** /outputs/
>  Continuing new work on step6-dev while awaiting feedback.

------

### **B) Forward-Development Branch**

After pushing your submission, immediately create a new development branch.

```bash
git checkout main
git pull
git checkout -b step6-dev
```

💡 **Rule:** `main` always represents the last *approved* version.

------

### **C) Handling Mentor Feedback**

If Akhil requests revisions:

```bash
git checkout step5-submission
git checkout -b step5-feedback

# apply fixes
git add .
git commit -m "Addressed Akhil's feedback on Step 5"
git push origin step5-feedback
```

Notify Akhil:

> “Updates applied — please review step5-feedback branch.”

Once approved:

```bash
git checkout main
git merge step5-feedback
git tag step5-approved-2025-10-08
git push origin main --tags
```

------

## ⚙️ Integrated Pre-Submission Cleanup Workflow

Before creating any submission branch, **run this cleanup sequence** to ensure your repo is free of transient files and detritus.

------

### 🧹 **Step-by-Step Cleanup Plan**

#### 1️⃣ Check Repository Status

```bash
git status --short
```

- Identify deleted, modified, or untracked files.
- Verify changes in `scripts/`, `docs/`, and `logs/` are intentional.

------

#### 2️⃣ Handle Deleted or Modified Docs

If something was accidentally deleted:

```bash
git restore docs/
```

If deletion is intentional cleanup:

```bash
git rm docs/<file>.md
```

------

#### 3️⃣ Remove Transient and Untracked Files

Common offenders:

```
audit_reports/*.csv
audit_reports/*.json
scripts/scripts.zip
outputs/*.parquet
```

Delete them safely:

```bash
del scripts\audit_reports\*.csv
del scripts\audit_reports\*.json
del scripts\scripts.zip
```

Then confirm:

```bash
git status
```

Only relevant `.py` or `.md` files should remain.

------

#### 4️⃣ Validate `.gitignore`

```bash
type .gitignore
```

Ensure it excludes:

- `/audit_reports/*`
- `/outputs/*`
- `.tmp`, `.bak`, `debug_*.log`

And **includes:**

- `/logs/pipeline.log` (mentor requirement)

------

#### 5️⃣ Stage and Commit Cleanup

```bash
git add .gitignore
git add scripts/base_step.py
git commit -m "Repo cleanup: remove transient artifacts and restore essential docs"
```

------

#### 6️⃣ Final Sanity Check

```bash
git status
```

Expected:

```
On branch step5-refactor
nothing to commit, working tree clean
```

------

#### 7️⃣ Merge Once Clean

Example:

```bash
git checkout main
git pull
git merge --no-ff step5-refactor -m "Merge stable Step 5 refactor branch"
git push origin main
```

------

### ✅ End-of-Step Submission Checklist

☑ Local run successful (`python main.py`)
 ☑ Logs + outputs verified
 ☑ Repo cleaned of temp artifacts
 ☑ Commit & push submission branch
 ☑ Message Akhil with branch name
 ☑ Update `docs/CHANGELOG.md`
 ☑ Branch forward for next step

------

## 🧱 Branch Map Reference

| Step | Submission Branch | Feedback Branch | Next Dev Branch | Tag Upon Approval          |
| ---- | ----------------- | --------------- | --------------- | -------------------------- |
| 5    | step5-submission  | step5-feedback  | step6-dev       | step5-approved-2025-10-08  |
| 6    | step6-submission  | step6-feedback  | step7-dev       | step6-approved-YYYY-MM-DD  |
| 7    | step7-submission  | step7-feedback  | step8-dev       | step7-approved-YYYY-MM-DD  |
| 8    | step8-submission  | step8-feedback  | step9-dev       | step8-approved-YYYY-MM-DD  |
| 9    | step9-submission  | step9-feedback  | step10-dev      | step9-approved-YYYY-MM-DD  |
| 10   | step10-submission | step10-feedback | *(end)*         | step10-approved-YYYY-MM-DD |

------

## Appendix A: Handling Mentor Feedback Drift

### 🧩 Scenario Overview

You’ve been developing happily on `step6-dev`, and your Step 5 submission is under review. While Akhil reviews, you continue work. Then he requests feedback changes, creating two diverging timelines:

- `step6-dev` = forward development
- `step5-feedback` = mentor revisions

You now need to merge Akhil’s fixes without losing your Step 6 progress.

------

### ⚙️ ASCII Visual Model

```
                (main)
                  |
   A --- B --- C --- D                ← main (Step 5 submission base)
                 \
                  \--- E --- F --- G  ← step6-dev (you build Step 6 here)
```

After feedback starts:

```
                 D
                 |\
                 | \--- E --- F --- G       ← step6-dev (still active)
                 \
                  H --- I                   ← step5-feedback (mentor fixes)
```

Once Akhil approves, you merge feedback:

```
(main)  A --- B --- C --- D --- H --- I
```

Then rebase development:

```
            A --- B --- C --- D --- H --- I --- E' --- F' --- G'
                                             ↑
                                             step6-dev (rebased)
```

------

### 🪜 Step-by-Step: “Freeze, Patch, Rebase”

1️⃣ **Freeze Development**
 Checkpoint your dev branch before switching to feedback work:

```bash
git checkout step6-dev
git add .
git commit -m "Checkpoint before feedback reconciliation"
git push origin step6-dev
```

2️⃣ **Patch Feedback Branch**
 Work on Akhil’s fixes in `step5-feedback`, commit, and push.

```bash
git checkout step5-feedback
git add .
git commit -m "Addressed mentor feedback (Step 5)"
git push origin step5-feedback
```

3️⃣ **Merge Feedback into Main**
 After approval:

```bash
git checkout main
git pull
git merge step5-feedback
git tag step5-approved-YYYY-MM-DD
git push origin main --tags
```

4️⃣ **Rebase Dev Branch**
 Replay your Step 6 commits on top of the updated main:

```bash
git checkout step6-dev
git rebase main
```

5️⃣ **Resolve Conflicts**
 If Git raises conflicts, resolve them calmly and continue:

```bash
git add .
git rebase --continue
git push origin step6-dev --force-with-lease
```

------

### 🧠 Mental Model

- `main` = “canon universe” (mentor-approved timeline)
- `step5-feedback` = “patch universe” (temporary)
- `step6-dev` = “parallel universe rebased onto canon”

Your goal: keep `main` as the single source of truth — always rebase dev branches onto it, never the other way around.

------

### 🪶 Analogy

Think of it like a **data pipeline patch cycle**:

1. Freeze ETL job (`checkpoint` commit)
2. Patch schema (`feedback` branch)
3. Deploy schema (`merge to main`)
4. Rerun ETL job on new schema (`rebase dev onto main`)

------

✅ **End State:**

- `main` = mentor-approved Step 5
- `step6-dev` = rebased and current
- `step5-feedback` = merged and can be deleted

------

🔑 **Maintained by:** GPT-5 (Springboard Data Bootcamp Coach)
 **Next Review:** after Step 6 submission prep
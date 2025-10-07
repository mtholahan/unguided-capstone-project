# ⚡ Quick Start Card – Mentor Submission Workflow

_Last updated: October 6, 2025_

## 🎯 Purpose
This is your **at-a-glance reference** for submitting each capstone step (Steps 5–10) to Akhil using Git branches.  
No explanation — just the commands and sequence.

---

## 🪜 Step-by-Step: Each Submission Cycle

### 1️⃣ Prepare Submission
Make sure your current code runs cleanly and logs are verified.

```bash
git checkout main
git pull
git checkout -b step5-submission
git add .
git commit -m "Step 5 submission: Local ETL Prototype (Steps 00–06)"
git push origin step5-submission
```

#### 📤 Tell Akhil:

> Branch: step5-submission
> Includes: Steps 00–06 under /Step_05_Scripts
> Logs: /logs/, Outputs: /outputs/
> Continuing work on step6-dev while awaiting feedback.

### 2️⃣ Keep Moving

Immediately branch forward to continue your next step’s work.

```bash
git checkout main
git pull
git checkout -b step6-dev
```

💡 main = always stable and approved
step6-dev = your sandbox for new development

### 3️⃣ Handle Feedback (if requested)

If Akhil gives you edits to apply:

```bash
git checkout step5-submission
git checkout -b step5-feedback

# apply fixes

git add .
git commit -m "Addressed Akhil's feedback on Step 5"
git push origin step5-feedback
```

#### 📤 Tell Akhil:

> Updates applied — please review step5-feedback branch.
>

When approved:

```bash
git checkout main
git merge step5-feedback
git tag step5-approved-2025-10-07
git push origin main --tags
```

### 4️⃣ Log It

Update docs/CHANGELOG.md:

```bash
## [Step 5 – Local ETL Prototype] – 2025-10-06

- Snapshot: Steps 00–06 OOP pipeline.
- Environment: Local `.venv`, verified outputs/logs.
- Awaiting mentor feedback.
```

Commit your doc update:

```bash
git add docs/CHANGELOG.md
git commit -m "Update changelog for Step 5 submission"
git push origin step5-submission
```

## 🧱 Branch Naming Map

| Purpose              | Example                   |
| -------------------- | ------------------------- |
| Mentor submission    | step5-submission          |
| Feedback edits       | step5-feedback            |
| New work (next step) | step6-dev                 |
| Approved milestone   | step5-approved-2025-10-07 |

### ✅ End-of-Step Checklist

☑ Local run successful (python main.py)
☑ Logs + outputs verified
☑ Commit & push submission branch
☑ Message Akhil with branch name
☑ Log submission in CHANGELOG.md
☑ Branch forward for next step

Keep this short, repeatable, and calm.
You don’t need to memorize it — just follow the rhythm each time.

---

### 🧠 GPT Suggestion
Print or pin this near your monitor.  
You’ll use this same checklist rhythm for every step from 5 through 10 — it’s your “auto-pilot” for mentor submissions.



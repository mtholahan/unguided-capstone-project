# 🧭 Mentor Submission Workflow – Branch-Based Methodology

_Last updated: October 6, 2025_

## 🎯 Purpose

This document defines how mentor submissions (to Akhil) are handled using **Git branches** rather than static `Step_XX_Scripts` folders alone.

The goal is to:
- Maintain reproducible **“snapshot” submissions** for review.
- Continue forward development without blocking progress.
- Allow easy feedback incorporation and traceable version history.

---

## 🧩 Overview

Each **capstone milestone (Step 5–10)** gets its own **dedicated branch** for mentor review.

You:
- Submit from a clean branch (e.g. `step5-submission`).
- Keep developing in parallel (e.g. `step6-dev`).
- Merge and tag only once mentor approval is received.

Akhil:
- Reviews the designated branch.
- Provides feedback without worrying about newer in-progress code.

---

## 🪜 Step-by-Step Workflow

### A) **Prepare and Tag the Submission**
Once a step’s deliverable is ready:

```bash
# ensure you're on main
git checkout main
git pull

# create submission branch
git checkout -b step5-submission

# stage and commit all updates
git add .
git commit -m "Step 5 submission: Local ETL Prototype (Steps 00–06)"

# push to GitHub
git push origin step5-submission
```

Then send Akhil a clear message:

> Subject: Step 5 Submission – Local ETL Prototype
> Branch: step5-submission
> Includes: Steps 00–06 under /Step_05_Scripts, verified end-to-end.
> Logs and outputs are in ../logs/ and ../outputs/.
> Continuing new work on step6-dev while awaiting feedback.

### B) Create a Forward-Development Branch

Immediately after pushing the submission:

```bash
git checkout main
git pull
git checkout -b step6-dev # the "-b" switch creates a branch
```

You can now sprint ahead freely.
The mentor review branch stays frozen — your new work won’t disturb it.

### C) Handling Feedback

If Akhil requests changes:

```bash
git checkout step5-submission
git checkout -b step5-feedback

# make revisions per feedback

git add .
git commit -m "Addressed Akhil's feedback on Step 5"
git push origin step5-feedback
```

Notify Akhil:

> "Updates applied — please review step5-feedback branch."
>

Once approved:

```bash
git checkout main
git merge step5-feedback
git tag step5-approved-2025-10-07
git push origin main --tags
```

Now your main branch represents the approved, reviewed version.

### D) Rinse and Repeat for Later Steps

Each milestone (Step 6–10) follows the same lifecycle:

| Step | Submission Branch | Feedback Branch | Dev Branch (Next Step | Tag Upon Approval          |
| ---- | ----------------- | --------------- | --------------------- | -------------------------- |
| 5    | step5-submission  | step5-feedback  | step6-dev             | step5-approved-2025-10-07  |
| 6    | step6-submission  | step6-feedback  | step7-dev             | step6-approved-YYYY-MM-DD  |
| 7    | step7-submission  | step7-feedback  | step8-dev             | step7-approved-YYYY-MM-DD  |
| 8    | step8-submission  | step8-feedback  | step9-dev             | step8-approved-YYYY-MM-DD  |
| 9    | step9-submission  | step9-feedback  | step10-dev            | step9-approved-YYYY-MM-DD  |
| 10   | step10-submission | step10-feedback | *(end)*               | step10-approved-YYYY-MM-DD |

## 🧱 Best Practices

- Always merge only approved branches into main.
- Keep your main branch stable — it should represent the last mentor-approved state.

- Include a short CHANGELOG.md entry for each submission:



- ```markdown
  ## [Step 5 – Local ETL Prototype] – Oct 6, 2025
  
  - Snapshot: Steps 00–06 OOP pipeline.
  - Environment: Local `.venv`, outputs and logs verified.
  - Awaiting mentor feedback.
  ```

  Tag every milestone (stepX-approved) — this is invaluable for tracking deliverables later.

## 💡 Why This Works

- Parallel safety: You can work ahead without waiting for reviews.
- Mentor clarity: Akhil knows exactly which branch to look at.

- Traceability: Every step is timestamped, tagged, and reproducible.

- Professionalism: Mirrors real-world DevOps workflows.


## 🧠 GPT Suggestion

Once the capstone is complete and all branches are merged and tagged:

- Consolidate all /Step_XX_Scripts/ directories into a single, modular /pipeline/ structure.

- This will transform your project from “educational milestone” style to “production-ready repo” — ideal for portfolio presentation
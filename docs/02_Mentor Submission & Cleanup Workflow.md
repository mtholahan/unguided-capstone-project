# 🐭 Mentor Submission & Cleanup Workflow (Dual-Mode Edition)

*Last updated: October 21, 2025*

------

## 🌟 Purpose

This document defines a unified Git branching and submission workflow for both **mentor-active** and **mentor-decoupled (paused)** states during the Springboard Data Engineering Boot Camp.

The goal:

- Maintain reproducible, review-ready branches for mentor submission
- Allow uninterrupted progress during pauses
- Preserve clean, traceable DevOps hygiene

------

## 🔄 Operating Modes

### **1. Mentor-Active Mode (Normal)**

You are paired with your mentor (Akhil). Each milestone step has a dedicated submission branch reviewed by your mentor. You proceed sequentially: dev → submission → feedback → approved.

Example flow:

```
main
 └─ step6-approved-candidate
      └─ step7-dev
           └─ step7-submission
```

- `main` always reflects the last mentor-approved step.
- You move forward only after Akhil reviews and approves the previous step.

### **2. Mentor-Decoupled Mode (Paused)**

You are in an **independent work phase** (e.g., boot camp pause). Mentor review is unavailable, but progress must continue.

**Guiding principle:** work forward in an isolated chain of step branches, preserving the approved baseline.

```
step6-approved-candidate (locked anchor)
 └─ step7-dev
      └─ step7-submission
           └─ step8-dev
                └─ step8-submission
```

**Key rules:**

- Tag the last mentor-approved step as the immutable anchor.
- Branch each step off the most recent `*-submission` branch.
- Do **not** rebase or merge into `main` until mentor recoupling.

When mentorship resumes:

1. Merge each approved submission branch back into `main` sequentially.
2. Delete decoupled branches after reconciliation.

------

## 🔍 Quick Reference: Branch Types

| Branch Type                | Purpose                                 | Merge Target           |
| -------------------------- | --------------------------------------- | ---------------------- |
| `stepX-dev`                | Working branch for development          | `stepX-submission`     |
| `stepX-submission`         | Frozen candidate for mentor review      | `main` (once approved) |
| `stepX-feedback`           | Temporary branch for fixes after review | `main`                 |
| `stepX-approved-candidate` | Final mentor-approved state             | `main`                 |
| `stepX-final-anchor`       | Tag for decoupled sprints               | *none* (immutable)     |

------

## 🚛 Mentor-Active Submission Lifecycle

### **1️⃣ Prepare Submission**

```bash
git checkout main
git pull
git checkout -b step7-submission
git add .
git commit -m "Step 7 submission: Deployment Architecture"
git push origin step7-submission
```

Notify Akhil with the branch name and summary.

------

### **2️⃣ Forward Development Branch**

Immediately continue forward progress:

```bash
git checkout main
git pull
git checkout -b step8-dev
```

**Rule:** main = last approved code.

------

### **3️⃣ Mentor Feedback Handling**

If feedback is received:

```bash
git checkout step7-submission
git checkout -b step7-feedback
# apply fixes
git add . && git commit -m "Addressed feedback for Step 7"
git push origin step7-feedback
```

After approval:

```bash
git checkout main
git merge step7-feedback
git tag step7-approved-2025-11-03
git push origin main --tags
```

------

## 🚴️‍♂️ Mentor-Decoupled (Paused) Workflow

### **A) Lock the last approved anchor**

```bash
git checkout step6-approved-candidate
git tag step6-final-anchor
git push origin step6-final-anchor
```

### **B) Create the next step’s dev branch**

```bash
git checkout -b step7-dev step6-approved-candidate
```

### **C) When Step 7 is ready for submission**

```bash
git checkout -b step7-submission step7-dev
git push origin step7-submission
```

### **D) Continue to Step 8 directly**

```bash
git checkout -b step8-dev step7-submission
```

Repeat until pause ends.

### **E) When mentorship resumes (post-pause)**

Merge back into main sequentially:

```bash
git checkout main
git merge step6-approved-candidate
git merge step7-submission
git merge step8-submission
```

Tag each one upon approval.

------

## 🧹 Pre-Submission Cleanup Checklist

1. Validate `git status` (no temp files)
2. Verify `.gitignore` excludes `/outputs/`, `/audit_reports/`, `.tmp`, `.bak`
3. Ensure logs exist under `/logs/pipeline.log`
4. Run local test: `python main.py`
5. Push submission branch cleanly

------

## 🔢 Bash Automation Template (Optional)

The following script automates the decoupled branch chain pattern. **Review before using** — it will tag anchors and create the next step’s branches.

```bash
#!/bin/bash
# decoupled_branch_chain.sh
# Automates step-based branching during mentor pauses

set -e

ANCHOR=${1:-step6-approved-candidate}
NEXT_STEP=${2:-step7}

if [ -z "$ANCHOR" ] || [ -z "$NEXT_STEP" ]; then
  echo "Usage: ./decoupled_branch_chain.sh <anchor-branch> <next-step-number>"
  exit 1
fi

# Tag the anchor
TAG_NAME="${ANCHOR}-final-anchor"
git checkout $ANCHOR
git tag $TAG_NAME
git push origin $TAG_NAME

# Create dev branch
git checkout -b step${NEXT_STEP}-dev $ANCHOR
git push origin step${NEXT_STEP}-dev

echo "\n✅ Created step${NEXT_STEP}-dev from $ANCHOR and tagged $TAG_NAME"

echo "\nNext: work on step${NEXT_STEP}-dev, then create step${NEXT_STEP}-submission when ready."
```

------

## 🔒 End-State Summary

| State      | Mentor Role                     | Branch Flow                               | Merge Policy                 |
| ---------- | ------------------------------- | ----------------------------------------- | ---------------------------- |
| **Active** | Approves each step sequentially | `dev → submission → feedback → main`      | Merge upon approval          |
| **Paused** | None (self-directed)            | `submission → next dev → next submission` | Hold merges until recoupling |

------

🔑 **Maintained by:** GPT-5 (Springboard Data Bootcamp Coach)
 **Next Review:** Post-pause, November 2025
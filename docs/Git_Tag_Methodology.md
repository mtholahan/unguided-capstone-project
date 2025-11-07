# 🏷️ **Git Tag Naming Standard — Unguided Capstone Project**

## 📘 Overview

This repository follows a **semantic + step-based** tag structure for clear, versioned milestones throughout the 11-step Capstone pipeline.

Each tag uses the format:

```
v<major>.<minor>.<patch>-step<##>-<label>
```

| Segment          | Meaning                          | Example                                                      |
| ---------------- | -------------------------------- | ------------------------------------------------------------ |
| `v0.9.3`         | Semantic version                 | Major = 0 (pre-release), Minor = 9 (iteration), Patch = 3 (fix or refinement) |
| `step08`         | Pipeline step (two-digit padded) | step08                                                       |
| `pandas-release` | Descriptive label for milestone  | pandas-release, enrichment, databricks-stable                |

------

## 🏷️ **Git Tag Summary — Unguided Capstone Project**

| **Tag Name**                  | **Type**    | **Tagger**   | **Date**   | **Annotation / Notes**                                       |
| ----------------------------- | ----------- | ------------ | ---------- | ------------------------------------------------------------ |
| `step5-milestone-2025-10-10`  | annotated   | Mark Holahan | 2025-10-10 | 📝 Step 5 Prototype complete: Discogs→TMDB ETL refactor (OAuth, matching validation) |
| `step5-milestone-2025-10-10b` | annotated   | Mark Holahan | 2025-10-10 | 📝 Final Step 5 submission snapshot after refactor merge (main aligned) |
| `step06A-verified`            | lightweight | Mark Holahan | 2025-10-15 | ⚠️ No annotation (lightweight tag)                            |
| `step6-milestone-2025-10-21`  | annotated   | Mark Holahan | 2025-10-21 | 📝 Step 6 submission – Scale Your Prototype (Databricks + ADLS integration) |
| `step6-approved-2025-10-21`   | lightweight | mtholahan    | 2025-10-21 | ⚠️ No annotation (lightweight tag)                            |
| `step6-final-anchor`          | lightweight | Mark Holahan | 2025-10-21 | ⚠️ No annotation (lightweight tag)                            |
| `step7-milestone-2025-10-22`  | annotated   | Mark Holahan | 2025-10-22 | 📝 Step 7 submission – Create the Deployment Architecture (Bicep orchestration + validation) |
| `step8-success`               | annotated   | Mark Holahan | 2025-10-25 | 📝 All Spark stages 01–05 validated on Ubuntu                 |
| `step8-stable`                | annotated   | Mark Holahan | 2025-10-29 | 📝 Stable Step 8 environment: Spark gateway fix verified, PySpark 3.5.3 runtime aligned with local Spark binary, end-to-end pipeline validated (`step_05_match_and_enrich`), dependencies frozen in `requirements_stable.txt`, ready for VM transfer and pytest integration |
| `v0.8-stable`                 | annotated   | Mark Holahan | 2025-10-30 | 📝 Unguided Capstone Step 8 baseline: orchestration verified, environment stable |



##  Proposed Tags

| Current Tag                   | Proposed Tag                       | Description                                  |
| ----------------------------- | ---------------------------------- | -------------------------------------------- |
| `v0.8-stable`                 | `v0.8.0-step01-tmdb-extract`       | First working TMDB Spark extraction verified |
| `step8-stable`                | `v0.9.8-step08-databricks-stable`  | Hybrid Databricks pipeline stabilized        |
| `step8-success`               | `v0.9.8-step08-pandas-success`     | Successful Pandas-based Step 08 validation   |
| `step7-milestone-2025-10-22`  | `v0.9.7-step07-enrichment`         | Genre + alt-title enrichment stable          |
| `step6-final-anchor`          | `v0.9.6-step06-match-tmdb`         | Final fuzzy-matching step complete           |
| `step6-approved-2025-10-21`   | `v0.9.6-step06-match-verified`     | Step 06 approval milestone verified          |
| `step6A-verified`             | `v0.9.6-step06A-tuning-verified`   | Intermediate tuning checkpoint               |
| `step5-milestone-2025-10-10b` | `v0.9.5-step05-filter-v2`          | Filter refinement iteration                  |
| `step5-milestone-2025-10-10`  | `v0.9.5-step05-filter-soundtracks` | Initial soundtrack filtering milestone       |

------

## 🧩 **Upcoming Tags (for consistency)**

| Planned Step | Tag                           | Description                           |
| ------------ | ----------------------------- | ------------------------------------- |
| Step 09      | `v0.9.9-step09-sql-sync`      | PostgreSQL sync verified              |
| Step 10      | `v1.0.0-step10-capstone-wrap` | Final pipeline + documentation ready  |
| Step 11      | `v1.0.1-step11-presentation`  | Presentation and submission milestone |

------

## ⚙️ **Migration Commands (Run in Git Bash)**

Fetch all tags first:

```
git fetch --tags
```

Then rename older ones to the new standard (example below for step 5 → 8):

```
# Step 5
git tag v0.9.5-step05-filter-soundtracks step5-milestone-2025-10-10
git tag v0.9.5-step05-filter-v2 step5-milestone-2025-10-10b

# Step 6
git tag v0.9.6-step06-match-tmdb step6-final-anchor
git tag v0.9.6-step06-match-verified step6-approved-2025-10-21
git tag v0.9.6-step06A-tuning-verified step6A-verified

# Step 7
git tag v0.9.7-step07-enrichment step7-milestone-2025-10-22

# Step 8
git tag v0.9.8-step08-databricks-stable step8-stable
git tag v0.9.8-step08-pandas-success step8-success
```

Push them all back up:

```
git push origin --tags
```

(Optional) delete old tags after confirming:

```
git tag -d step8-success step8-stable step7-milestone-2025-10-22
git push origin :refs/tags/step8-success :refs/tags/step8-stable :refs/tags/step7-milestone-2025-10-22
```

------

## 🧭 **Best Practices**

1. Always create annotated tags for traceability:

   ```
   git tag -a v0.9.8-step08-databricks-stable -m "Step 08 hybrid Databricks pipeline stabilized and verified"
   ```

2. Use lowercase, hyphenated labels (`eda`, `pandas-release`, `databricks-stable`).

3. Increment patch numbers (`v0.9.3 → v0.9.4`) for small fixes.

4. Increment minor numbers (`v0.9 → v0.10`) for new Capstone steps.

5. Reserve `v1.0.0` for your final, fully validated Capstone submission.

## Get Git Tag Deets

### 🧭 **Option 1 — Sort Local Tags by Creation Date**

If you’ve already fetched all remote tags:

```bash
git fetch --tags
git for-each-ref --sort=creatordate --format '%(creatordate:short)  %(refname:short)' refs/tags
```

✅ **Example output:**

```ba
2024-12-11  v0.8.1-step01-tmdb
2025-01-22  v0.9.0-step02-discogs
2025-11-02  v0.9.3-step03-pandas-release
```

This shows creation date (in your local tag metadata), followed by tag name.

------

### 🌐 **Option 2 — Sort Remote Tags by Commit Date**

If you want the order based on the *commit the tag points to* (which works even when `creatordate` is missing):

```bash
git ls-remote --tags origin \
  | grep -v '\^{}' \
  | while read hash ref; do 
        tag=${ref#refs/tags/}
        date=$(git log -1 --format=%ai $hash 2>/dev/null)
        echo "$date  $tag"
    done | sort
```

✅ **Output example:**

```
2025-01-22 13:20:44 -0500  v0.9.0-step02-discogs
2025-03-18 17:05:09 -0400  v0.9.1-step02-patch
2025-11-02 03:47:00 -0400  v0.9.3-step03-pandas-release
```






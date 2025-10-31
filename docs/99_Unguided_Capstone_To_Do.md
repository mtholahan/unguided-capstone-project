# ✅ Unguided Capstone – ToDo.md

## 🎯 Purpose
Track post–Step 8 improvements and deployment refinements for the
Unguided Capstone project (Azure VM + Spark).

---

## 🧱 1. Environment & Configuration

| Item                                                         | Status | Notes                                           |
| ------------------------------------------------------------ | ------ | ----------------------------------------------- |
| `.env` centralized and synced                                | ✅ Done | Ensure same version exists on both local and VM |
| Add safe defaults to `.env` (`PIPELINE_OUTPUT_DIR`, `PIPELINE_METRICS_DIR`, `SPARK_HOME`, `PYSPARK_PYTHON`) | ✅ Done | Confirm `echo $PIPELINE_OUTPUT_DIR` on VM       |
| Verify `.env` sourced automatically in all run scripts       | ⏳ Next | Add `source "$PIPELINE_ROOT/.env"` to wrappers  |

---

## ⚙️ 2. Runtime Scripts

| Task                                                         | Status     | Notes                                                   |
| ------------------------------------------------------------ | ---------- | ------------------------------------------------------- |
| Create `run_pipeline_safe.sh`                                | ✅ Complete | Wraps `spark-submit main.py` with correct PATH and logs |
| Integrate `.env` sourcing in `run_pipeline_safe.sh`          | ⏳ Next     | Ensures consistent env load                             |
| Refactor `deploy_to_azure_test.sh` to call `run_pipeline_safe.sh` remotely | ⏳ Planned  | Remove inline spark block once verified                 |
| Add `exit_code` propagation for CI/CD use                    | ⏳ Planned  | Return VM job status to local shell                     |

---

## ☁️ 3. GitHub Integration (Step 9+)

| Task                                               | Status    | Notes                                         |
| -------------------------------------------------- | --------- | --------------------------------------------- |
| Add GitHub pull logic to `deploy_to_azure_test.sh` | ⏳ Planned | Replace tarball deploy with `git pull`        |
| Configure SSH deploy key on VM                     | ⏳ Planned | Read-only key → no credential storage         |
| Log latest commit hash in pipeline_run.log         | ⏳ Planned | `git rev-parse HEAD >> logs/pipeline_run.log` |
| Optional fallback: use tarball if Git fails        | ⏳ Planned | Hybrid reliability model                      |

---

## 🔬 4. Testing & Validation

| Task                                                        | Status                      | Notes |
| ----------------------------------------------------------- | --------------------------- | ----- |
| Confirm Step 03 writes outputs to correct path              | ✅ Confirm after `.env` sync |       |
| Verify Step 05 reads `tmdb_discogs_candidates_extended.csv` | ⏳ Next run                  |       |
| Validate pytest suite (`pytest_report.log`, coverage %)     | ⏳ Step 8 deliverable        |       |
| Consolidate logs to `/logs/` and `/test_results/`           | ✅ In place                  |       |

---

## 🧩 5. Future Enhancements

- [ ] Add timestamped log rotation (`logs/pipeline_run_YYYYMMDD_HHMM.log`)
- [ ] Add Slack / Teams webhook for remote job status
- [ ] Integrate Azure Blob copy step for output artifacts
- [ ] Build simple Makefile with `make deploy`, `make run`, `make test`
- [ ] Set up GitHub Actions for automatic deploy/test to VM

---

## 📅 Step 8 Submission Checklist

- [ ] `pipeline_run.log` uploaded with successful Spark execution
- [ ] `pytest_report.log` showing pass/fail + coverage summary
- [ ] Screenshot of final VM test run (✅ Spark success)
- [ ] Brief summary of test suite and environment setup in slide deck

---

**Next Execution Goal:**
After `.env` sync and successful pipeline run, confirm CSV creation and submit Step 8 deliverables.
Then begin Step 9 refactor: integrate GitHub-based deploy and full CI/CD chain.

---

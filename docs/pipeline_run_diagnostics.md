# 🧭 Unguided Capstone — Azure Pipeline Run Diagnostics

This guide walks you through running your data pipeline on the Azure VM, verifying environment parity, and resolving common failures.

---

## ⚙️ Step-by-Step Execution Guide

### 1️⃣ Local Environment Prep
<details>
<summary><b>Clean and Rebuild (Local ASUS)</b></summary>

**Run:**

```
make clean && make rebuild
```

Expected:
✅ “Local environment rebuilt.”

If it fails:

Run `bash rebuild_venv.sh` manually.

Verify `pyspark_venv311` exists locally.

Verify:

```
ls -lh pyspark_venv311/
```


2️⃣ Commit and Push Latest Code

<details> <summary><b>Ensure Remote Sync</b></summary>
Run:
```
git add -A && git commit -m "Sync before Azure pipeline test"
git push
```


Expected:
✅ Commit message logged, push succeeds.

If it fails:

- Check SSH credentials.

- Confirm you’re on main branch.

- Use `git status` to ensure nothing’s left untracked.



3️⃣ SSH to Azure VM

<details> <summary><b>Access VM Securely</b></summary>
Run (from local ASUS):
```
ssh -i ~/.ssh/ungcapvm01-key.pem azureuser@172.190.228.102
```

Expected:
Prompt: `azureuser@ungcapvm01:~$`

If it fails:

- Run `chmod 600 ~/.ssh/ungcapvm01-key.pem`

- Verify VM IP: `az vm list-ip-addresses`



4️⃣ Project Setup on VM

<details> <summary><b>Confirm Directory and Rebuild Environment</b></summary>
Run:
```
cd ~/unguided-capstone-project
bash rebuild_venv.sh --force
```


Expected:
✅ “Environment rebuilt successfully.”

If it fails:

- Confirm `requirements.txt` exists.

- Sync from local using `make export`.



5️⃣ Activate Python Virtual Environment

<details> <summary><b>Activate and Validate</b></summary>
Run:
```
source pyspark_venv311/bin/activate
python -V
pip list | grep pyspark
```


Expected:
✅ Python 3.11.x and PySpark 3.5.3 listed.

If it fails:
Rebuild venv again:

```
bash rebuild_venv.sh --force
```


6️⃣ Environment Validation

<details> <summary><b>Run Preflight Checks</b></summary>
Run:
```
bash check_env.sh
```


Expected:
✅ “Environment check passed. Safe to run pipeline.”

Common Failures:

| Message                   | Root Cause                    | Fix                                           |
| ------------------------- | ----------------------------- | --------------------------------------------- |
| ⚠️ No virtualenv active    | Forgot to activate venv       | `source pyspark_venv311/bin/activate`         |
| ❌ SPARK_HOME not set      | Spark not unpacked or missing | `export SPARK_HOME=~/spark-3.5.3-bin-hadoop3` |
| ❌ rapidfuzz not installed | Missing dependency            | `pip install -r requirements.txt`             |

7️⃣ Run the Pipeline

<details> <summary><b>Execute Safely via Script</b></summary>
Run:
```bash
bash run_pipeline_safe.sh
```


Expected:
✅ Log entry: “Pipeline finished at …”

If it fails early:

- Check `check_env.sh` output.

- Re-activate venv.

- Ensure Spark cluster paths exist.


Monitor Logs:

```bash
tail -f logs/pipeline_run_*.log
```

8️⃣ Verify Output and Logs

<details> <summary><b>Check Generated Data</b></summary>
Run:
```bash
ls -lh data/intermediate/
grep -E "ERROR|Exception" logs/pipeline_run_*.log
```


Expected:

- Output files appear with non-zero size.

- No errors or exceptions in logs.


If empty:
Pipeline didn’t write → check log lines above last Spark stage for cause.

| 🛑 Symptom                               | Likely Root Cause      | Quick Fix                                                    | Verify                            |
| --------------------------------------- | ---------------------- | ------------------------------------------------------------ | --------------------------------- |
| `check_env.sh` → “No virtualenv active” | venv not sourced       | `source pyspark_venv311/bin/activate`                        | `echo $VIRTUAL_ENV`               |
| “spark-submit: command not found”       | `$SPARK_HOME` unset    | `export SPARK_HOME=~/spark-3.5.3-bin-hadoop3 && export PATH=$SPARK_HOME/bin:$PATH` | `spark-submit --version`          |
| “rapidfuzz not installed”               | missing dependency     | `pip install -r requirements.txt`                            | `pip show rapidfuzz`              |
| “AnalysisException” in Spark log        | bad input path         | verify dataset path in `pipeline_main.py`                    | `head data/raw/*`                 |
| “Permission denied” on VM               | wrong file permissions | `chmod -R u+rwx ~/unguided-capstone-project`                 | rerun command                     |
| No `logs/` dir                          | directory missing      | `mkdir -p logs`                                              | rerun `bash run_pipeline_safe.sh` |

🧾 Post-Run Verification Checklist
After a successful run:

-  ✅ Log file created in logs/

-  ✅ No “Exception” lines in the last 20 log entries

-  ✅ data/intermediate/ contains processed output

-  ✅ Git branch clean (git status)

-  ✅ Step 9 PDF ready for upload (next phase)


Document maintained: October 2025
Maintainer: Springboard Data Bootcamp Coach (GPT-5)

## 🧭 Big Picture Reset — What You Have Now

| Tool / Script         | What It Actually Does                                        | Where to Run It                                  |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------ |
| **`scripts/main.py`** | Runs the pipeline locally (your ETL steps).                  | Local WSL shell with `venv` activated.           |
| **`deploy_to_vm.sh`** | Syncs latest code from local → Azure VM via `rsync`, validates Spark, Python, and pytest on the VM. | Local WSL shell (it SSHs into VM automatically). |
| **`rebuild_venv.sh`** | Rebuilds your local Python environment from scratch (PySpark, pandas, etc.). | Local WSL shell.                                 |
| **`vm_quickops.sh`**  | (Optional) Performs quick Spark + pytest checks remotely after deployment. | Runs automatically at the end of deploy_to_vm.   |

------

## ⚙️ How to Run the Pipeline (Local vs. VM)

### 🧩 **Local (WSL or Linux)**

From your project root:

```
source ~/pyspark_venv311/bin/activate
python3 scripts/main.py
```

- This uses your fixed environment (`clean_path` etc.).
- Use this for Step 8 pytest validation or data pipeline debugging.

✅ **If Spark prints its version and runs enrichment logic — you’re good.**

------

### ☁️ **On the VM**

You don’t need to manually SSH anymore.
 Just use the **deployment script**:

```
bash deploy_to_vm.sh
```

It will:

1. `rsync` all your updated files (excluding venvs, data, etc.)
2. SSH into the VM and:
   - Activate the VM’s `~/pyspark_venv311`
   - Validate Spark, Python, and pytest environment
3. Optionally trigger `vm_quickops.sh` for auto-tests

You’ll see a summary block like:

```
============================
ENV VALIDATION SUMMARY
============================
Python Env:  ✅
Spark Setup: ✅
Pytest Run:  ✅
============================
== VM VALIDATION COMPLETE ==
```
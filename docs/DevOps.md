## ⚙️ **Environment Sync Protocol — Unguided Capstone**

**Purpose:**
 To maintain reproducible, low-latency synchronization between the **local dev machine (ASUS Ubuntu)** and the **Azure VM (`ungcapvm01`)** while avoiding redundant transfers or Git pollution.

------

### 🧩 1. Git Sync — Version-Controlled Code

**Use when:**

- Updating reviewed, tested, or mentor-visible code.
- Pushing structured progress checkpoints (`stepX-dev` → `stepX-submission`).

**Scope:**
 `scripts/`, `scripts_spark/`, `config/`, `docs/`, and other tracked assets.

**Typical workflow:**

```
git add .
git commit -m "Step 8: stabilized orchestration and logging"
git push origin step8-dev
```

------

### 🗜️ 2. Tarball Sync — Immutable Snapshots

**Use when:**

- Archiving a completed step or milestone.
- Sharing a frozen project snapshot independent of Git.
- Migrating between machines or VMs.

**Command:**

```
tar -czvf step8_submission_$(date +%Y%m%d).tar.gz ~/Projects/unguided-capstone-project
```

**Notes:**

- Store in `archive/` or offload to Azure Blob.
- Avoid committing tarballs to Git.

------

### ⚡ 3. Rsync — Fast Incremental Deployment

**Use when:**

- Syncing local work to VM for Spark execution/testing.
- Avoiding full environment rebuilds between runs.

**Command:**

```
rsync -avz \
  --exclude 'data/metrics/' \
  --exclude '__pycache__/' \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude 'pyspark_venv311/' \
  -e "ssh -i ~/.ssh/ungcapvm01-key.pem" \
  ~/Projects/unguided-capstone-project/ azureuser@172.190.228.102:~/Projects/
```

**Verification:**

```
ssh -i ~/.ssh/ungcapvm01-key.pem azureuser@172.190.228.102
ls ~/Projects/unguided-capstone-project
```

**Recommended:**
 Save this command as `deploy_to_vm.sh` (executable) to simplify sync runs.

------

### 🧠 4. Sync Decision Matrix

| Situation                         | Best Tool             | Reason                        |
| --------------------------------- | --------------------- | ----------------------------- |
| Code & pipeline logic updates     | **Git**               | Version tracking              |
| Dataset / metrics update only     | **Rsync**             | Ignore Git pollution          |
| End-of-step archive               | **Tarball**           | Immutable backup              |
| Cross-region or mentor submission | **Git** + **Tarball** | Reproducibility + portability |

------

## ✅ **Post-Sync Validation (VM Environment Parity Check)**

**Purpose:**
 Verify that the Azure VM environment mirrors local development before executing Spark or pytest test suites.

------

### **1. SSH into the VM**

```
ssh -i ~/.ssh/ungcapvm01-key.pem azureuser@172.190.228.102
cd ~/Projects/unguided-capstone-project
```

------

### **2. Activate the Correct Python Environment**

```
source ~/pyspark_venv311/bin/activate
which python
python --version
```

✅ Confirm: version matches local (e.g. `Python 3.11.x`).

------

### **3. Verify Spark Availability**

```
pyspark --version
```

✅ Confirm: same minor version as local Spark; no “JAVA_HOME” or “ClassNotFound” errors.

*(Exit with `Ctrl+D` once Spark shell loads.)*

------

### **4. Validate Package Parity**

```
pip freeze | grep -E 'pyspark|pytest|python-dotenv|coverage'
```

✅ Confirm: versions match `requirements.txt`.

------

### **5. Check Environment Variables**

```
echo $SPARK_HOME
echo $PYTHONPATH
```

✅ Confirm: both paths resolve to `/home/azureuser/Projects/unguided-capstone-project` or corresponding cluster directories.

------

### **6. Run a Dry-Run Pipeline Check**

```
bash run_pipeline_safe.sh --dry-run
```

✅ Confirm:

- Spark Session initializes successfully.
- Logging path resolves under `logs/`.
- No permission or module-import errors.

------

### **7. Verify Test Accessibility**

```
pytest -q scripts/tests/test_env_validation.py
```

✅ Confirm:

- Tests discovered correctly (`collected X items`).
- No ImportErrors.

------

### **8. Optional Metrics Check**

```
ls data/metrics/
tail -n 10 logs/pipeline.log
```

✅ Confirm: metrics/logs writing to expected locations, not root.

------

### **9. Snapshot Results**

Create a local marker log for Step 8 validation:

```
echo "$(date) : VM parity check passed" >> docs/vm_validation_history.log
```

------

This routine ensures both environments stay in sync before each test-suite or full Spark orchestration run.






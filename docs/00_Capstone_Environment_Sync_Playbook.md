# Synchronizing Environments:

**Run the trifecta** exactly as shown:

1. **Anchor**

   ```
   git pull && bash rebuild_venv.sh
   ```

2. **Execute**

   ```
   bash rebuild_venv.sh --export
   ssh azureuser@<VM_IP> "bash ~/rebuild_venv.sh --force"
   ```

3. **Validate**

   ```
   bash check_env.sh
   bash run_pipeline_safe.sh
   ```

Run that sequence end-to-end.
 Each phase will:

- Rebuild your environment deterministically.
- Auto-log the post-check to `sync_log.md`.
- Confirm parity between ASUS ↔ VM ↔ Git.

## Alternatively:









🧩 1️⃣ Why this happens (even when you wrote everything)
❗ Fragmented context

You have three parallel execution contexts:

Local (ASUS) — where files are edited, Python deps are installed, and .env is current.

VM (ungcapvm01) — where deployment runs.

Git repo — the canonical source of truth, but not always in sync with either.

Every time one of those diverges (e.g., a local .env updated but not re-tarred to VM, or a module installed locally but not in the VM’s requirements_stable.txt), your deploy pipeline becomes a game of “guess which copy is real.”

🧩 2️⃣ The real errors you’re seeing
❌ No such file: pipeline_main.py

→ A stale filename inside your runner script.
Root cause: inconsistent paths between refactors.

❌ ModuleNotFoundError: rapidfuzz

→ The VM’s virtual environment is missing that dependency.
Root cause: requirements_stable.txt doesn’t match your local install.

🧩 3️⃣ The fix — move from reactive → anchored development
✅ Step 1 — Environment parity (never ad hoc installs again)

On ASUS (where it works), export the current environment:

source pyspark_venv311/bin/activate
pip freeze > requirements_stable.txt


Commit and push that file.
Then on the VM, always rebuild venv from that file:

source pyspark_venv311/bin/activate
pip install -r requirements_stable.txt


That ensures every missing module (like rapidfuzz) installs automatically.

✅ Step 2 — One canonical path reference

Add a single constant to .env:

PIPELINE_ENTRYPOINT=scripts/main.py


Then make your runner use it:

spark-submit --master local[4] "$PIPELINE_ROOT/$PIPELINE_ENTRYPOINT"


Now renames never break execution again.

✅ Step 3 — Two-command deployment workflow

On ASUS:

tar czf deploy_payload.tgz .env requirements_stable.txt run_pipeline_safe.sh
scp -i ~/.ssh/ungcapvm01-key.pem deploy_payload.tgz azureuser@172.190.228.102:/home/azureuser


On VM:

tar xzf deploy_payload.tgz -C ~/unguided-capstone-project
bash ~/unguided-capstone-project/run_pipeline_safe.sh


That’s your atomic sync — no guessing, no partial transfers.

✅ Step 4 — Logging discipline

At the top of every pipeline run, echo environment info into the log:

echo "RUN_CONTEXT=$(hostname) | USER=$USER | PWD=$(pwd)" | tee -a "$LOGFILE"


You’ll always know where and under which env a run executed.

✅ Step 5 — Git commit discipline

Every structural change (path, env var, dependency) must come with:

git commit -am "Update: sync pipeline_main→scripts/main.py, freeze env"


That ensures the next deploy pulls all the fixes you already made.

Once these are in place:

Paths don’t drift.

Dependencies install automatically.

VM behavior mirrors local every time.

Debugging drops from hours to minutes.

## 🧩 Layer 1 — Structural Reinforcement

6️⃣ .env schema lock

Maintain a .env.template file in Git that defines every required variable (with placeholders and comments).
Example:

# === Core Paths ===
PIPELINE_ROOT=/home/azureuser/unguided-capstone-project
PIPELINE_ENTRYPOINT=scripts/main.py
PIPELINE_OUTPUT_DIR=${PIPELINE_ROOT}/data/intermediate
PIPELINE_LOG_DIR=${PIPELINE_ROOT}/logs

# === Spark ===
SPARK_HOME=/opt/spark
PYSPARK_PYTHON=${PIPELINE_ROOT}/pyspark_venv311/bin/python


Before each deploy, validate it with:

dotenv-linter run .env


(or a small Python validator that checks for missing keys)

7️⃣ Automated pre-flight check script

Create a lightweight diagnostic tool, e.g. check_env.sh:

#!/bin/bash
echo "Verifying Capstone environment..."
python3 --version
echo "SPARK_HOME=$SPARK_HOME"
command -v spark-submit || echo "spark-submit not found"
ls -ld data logs || echo "data or logs missing"
pip list | grep -E "pyspark|rapidfuzz"


Run it automatically at the start of run_pipeline_safe.sh to catch misconfigurations before Spark even starts.

8️⃣ Version tagging

Include version headers in each log:

git describe --always --tags >> "$LOGFILE" 2>&1


This lets you trace which commit produced any result.

🧩 Layer 2 — Operational Safety
9️⃣ Lock dependency versions

In requirements_stable.txt, freeze exact versions:

pyspark==3.5.3
rapidfuzz==3.9.2
python-dotenv==1.0.1


That makes deployments reproducible, eliminating “works-on-my-machine.”

🔟 Pipeline step health logging

Wrap each ETL step with explicit success/fail log messages, so one bad transformation doesn’t hide among Spark logs:

try:
    run_step_05_match_and_enrich()
    logger.info("✅ Step 05 completed")
except Exception as e:
    logger.exception("❌ Step 05 failed: %s", e)
    raise

## 🧩 Layer 3 — Developer Quality of Life

11️⃣ Repo hygiene

Keep scripts/, scripts_spark/, and data/ well-scoped (no cross-imports).

Add a Makefile or simple CLI:

make deploy
make run
make clean


so anyone can reproduce your workflow with two commands.

12️⃣ Log rotation & retention

Automatically compress and timestamp logs older than 7 days to avoid clutter:

find logs -type f -mtime +7 -exec gzip {} \;

13️⃣ Documentation snapshot

Add a README_VM_SETUP.md with:

environment variables

install commands

deployment sequence
so future you (or teammates) never re-learn this by debugging.

✅ Net result

Implementing these 13 as standard practice means:

Every deploy is predictable

Debugging is deterministic

The system remains self-documenting

## 🧩 **Conventional Commit Prefixes (Cheat Sheet)**

| Prefix        | Meaning                                                      | Example                                        |
| ------------- | ------------------------------------------------------------ | ---------------------------------------------- |
| **feat:**     | Introduces a new feature                                     | `feat(pipeline): add Spark retry logic`        |
| **fix:**      | Fixes a bug or error                                         | `fix(env): correct Spark path detection`       |
| **chore:**    | “Housekeeping” changes — build scripts, CI, environment, docs, etc. | `chore(env): unify local & VM runtime scripts` |
| **refactor:** | Improves existing code structure without changing behavior   | `refactor(pipeline): streamline load step`     |
| **docs:**     | Documentation only                                           | `docs: add pipeline diagnostics guide`         |
| **test:**     | Adds or modifies tests                                       | `test: expand pytest coverage for Spark jobs`  |
| **style:**    | Formatting or stylistic changes (no code logic impact)       | `style: apply Black formatting`                |


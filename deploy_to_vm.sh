#!/usr/bin/env bash
# =========================================================
# 🧩  Unguided Capstone – Deploy to Azure VM
# Version: 2.5 (final)
# =========================================================

VM_USER="azureuser"
VM_HOST="172.190.228.102"
SSH_KEY="$HOME/.ssh/ungcapvm01-key.pem"
LOCAL_PATH="$HOME/Projects/unguided-capstone-project"
REMOTE_BASE="~/Projects"
LOG_FILE="docs/sync_log.md"

# --- Safety checks ---
if [ ! -f "$SSH_KEY" ]; then
  echo "❌ SSH key not found at $SSH_KEY"; exit 1
fi
if [ ! -d "$LOCAL_PATH" ]; then
  echo "❌ Local project not found at $LOCAL_PATH"; exit 1
fi
if [ ! -f "$LOG_FILE" ]; then
  echo "# 🧾 Capstone Sync Log" > "$LOG_FILE"
  echo "" >> "$LOG_FILE"
  echo "| Timestamp (UTC) | Context | Action | Result |" >> "$LOG_FILE"
  echo "| --------------- | -------- | ------- | ------- |" >> "$LOG_FILE"
fi

# =========================================================
# 🔄 Rsync (flattened, correct path)
# =========================================================
echo "🚀 Syncing project to VM..."
rsync -avz \
  --exclude 'data/metrics/' \
  --exclude '__pycache__/' \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude 'pyspark_venv311/' \
  --exclude 'env/' \
  --exclude 'venv/' \
  -e "ssh -i ${SSH_KEY}" \
  "${LOCAL_PATH}/" "${VM_USER}@${VM_HOST}:${REMOTE_BASE}/unguided-capstone-project/"

RSYNC_EXIT=$?
if [ $RSYNC_EXIT -ne 0 ]; then
  echo "| $(date -u '+%Y-%m-%d %H:%M') | VM (${VM_HOST}) | rsync deploy | ❌ failed |" >> "$LOG_FILE"
  echo "❌ Rsync failed. Check SSH or network."
  exit 1
else
  echo "| $(date -u '+%Y-%m-%d %H:%M') | VM (${VM_HOST}) | rsync deploy | ✅ synced successfully |" >> "$LOG_FILE"
fi

# =========================================================
# 🧪 Validation (auto-detect + diagnostic + summary)
# =========================================================
echo "🔍 Running remote validation checks..."
ssh -i "${SSH_KEY}" ${VM_USER}@${VM_HOST} bash << 'EOF'
set -e
echo "== VM VALIDATION START =="

STATUS_PYTHON="⚠"
STATUS_SPARK="⚠"
STATUS_TESTS="⚠"

# --- Detect project directory ---
for path in "$HOME/unguided-capstone-project" "$HOME/Projects/unguided-capstone-project"; do
  if [ -d "$path" ]; then
    PROJECT_DIR="$path"
    break
  fi
done

if [ -z "$PROJECT_DIR" ]; then
  echo "❌ Project not found."; exit 1
fi

echo "Detected project dir: $PROJECT_DIR"
cd "$PROJECT_DIR" || { echo "❌ cd failed."; exit 1; }

# --- Python environment ---
if [ -f "$HOME/pyspark_venv311/bin/activate" ]; then
  source "$HOME/pyspark_venv311/bin/activate"
fi
echo "Python:" && python --version && STATUS_PYTHON="✅" || STATUS_PYTHON="❌"

# --- Spark validation ---
if pyspark --version >/dev/null 2>&1; then
  echo "Spark OK"; STATUS_SPARK="✅"
else
  echo "⚠️ Spark version check failed."; STATUS_SPARK="❌"
fi

# --- Test validation ---
if [ -f "$PROJECT_DIR/scripts/tests/test_env_validation.py" ]; then
  echo "✅ Found test at scripts/tests/test_env_validation.py"
  cd "$PROJECT_DIR/scripts"

  # 🟢 Load environment variables from .env if present
  if [ -f "$PROJECT_DIR/.env" ]; then
    echo "📦 Loading .env variables..."
    export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
  else
    echo "⚠️ No .env file found in $PROJECT_DIR"
  fi

if "$HOME/pyspark_venv311/bin/pytest" -q tests/test_env_validation.py; then
  STATUS_TESTS="✅"
else
  STATUS_TESTS="❌"
fi

elif [ -f "$PROJECT_DIR/tests/test_env_validation.py" ]; then
  echo "✅ Found test at tests/test_env_validation.py"
  cd "$PROJECT_DIR"

  if [ -f "$PROJECT_DIR/.env" ]; then
    echo "📦 Loading .env variables..."
    export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
  else
    echo "⚠️ No .env file found in $PROJECT_DIR"
  fi

  if pytest -q tests/test_env_validation.py; then
    STATUS_TESTS="✅"
  else
    STATUS_TESTS="❌"
  fi
else
  echo "❌ No test_env_validation.py found."
  STATUS_TESTS="❌"
fi


# --- Summary Block ---
echo ""
echo "============================"
echo "ENV VALIDATION SUMMARY"
echo "============================"
printf "Python Env:  %s\n" "$STATUS_PYTHON"
printf "Spark Setup: %s\n" "$STATUS_SPARK"
printf "Pytest Run:  %s\n" "$STATUS_TESTS"
echo "============================"
echo "== VM VALIDATION COMPLETE =="
EOF

VALIDATION_EXIT=$?
if [ $VALIDATION_EXIT -eq 0 ]; then
  echo "| $(date -u '+%Y-%m-%d %H:%M') | VM (${VM_HOST}) | env validation | ✅ passed |" >> "$LOG_FILE"
else
  echo "| $(date -u '+%Y-%m-%d %H:%M') | VM (${VM_HOST}) | env validation | ⚠️ issues detected |" >> "$LOG_FILE"
fi

echo "✅ Deployment + validation complete."
echo "🧾 Log updated at ${LOG_FILE}"
# =========================================================

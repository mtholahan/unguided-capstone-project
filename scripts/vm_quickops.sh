#!/usr/bin/env bash
# ==========================================================
# 🧭 UNGUIDED CAPSTONE – VM QUICKOPS SCRIPT (v2)
# Connects to VM, activates venv, validates environment,
# and optionally runs pytest.
#
# Usage:
#   ./vm_quickops.sh               # interactive
#   ./vm_quickops.sh --auto-test   # non-interactive pytest run
# ==========================================================

VM_HOST="172.190.228.102"
VM_USER="azureuser"
SSH_KEY="$HOME/.ssh/ungcapvm01-key.pem"
PROJECT_DIR="~/Projects/unguided-capstone-project"
RUN_MODE="interactive"

# --- Parse arguments ---
if [[ "$1" == "--auto-test" ]]; then
  RUN_MODE="auto"
fi

echo "🔐 Connecting to $VM_USER@$VM_HOST ..."
ssh -i "$SSH_KEY" "$VM_USER@$VM_HOST" bash <<"EOF"
set -euo pipefail

echo "=============================================="
echo "🚀 Connected to $(hostname) at $(date -u)"
echo "=============================================="

cd ~/Projects/unguided-capstone-project || {
  echo "❌ Project directory not found."; exit 1;
}
echo "📁 Location: $(pwd)"

# ---- Activate venv ----
if [ -d ~/pyspark_venv311 ]; then
  source ~/pyspark_venv311/bin/activate
  echo "🐍 Python $(python --version)"
else
  echo "⚠️ Python venv not found at ~/pyspark_venv311"
  exit 1
fi

# ---- Quick environment verification ----
echo "🔎 Checking Spark..."
spark-submit --version | grep "version" || echo "⚠️ Spark not detected."

echo "🔎 Checking pytest..."
pytest --version || echo "⚠️ pytest missing in venv."

# ---- Run tests (conditional) ----
if [[ "$RUN_MODE" == "auto" ]]; then
  echo "🧪 Auto-mode enabled — running pytest..."
  pytest -q --disable-warnings || echo "⚠️ Some tests failed."
else
  read -p "▶️  Run pytest now? [y/N]: " RUN_TESTS
  if [[ "$RUN_TESTS" =~ ^[Yy]$ ]]; then
    echo "🧪 Running tests..."
    pytest -q --disable-warnings || echo "⚠️ Some tests failed."
  fi
fi

echo "✅ VM QuickOps completed successfully at $(date -u)"
EOF

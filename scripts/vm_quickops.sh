#!/usr/bin/env bash
# ==========================================================
# 🧭 UNGUIDED CAPSTONE – VM QUICKOPS SCRIPT
# Connects to VM, activates venv, validates environment,
# runs quick health check & optional test suite
# ==========================================================

VM_HOST="172.190.228.102"
VM_USER="azureuser"
SSH_KEY="$HOME/.ssh/ungcapvm01-key.pem"
PROJECT_DIR="~/Projects/unguided-capstone-project"

echo "🔐 Connecting to $VM_USER@$VM_HOST ..."
ssh -i "$SSH_KEY" "$VM_USER@$VM_HOST" bash <<'EOF'
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

# ---- Optional test suite run ----
read -p "▶️  Run pytest now? [y/N]: " RUN_TESTS
if [[ "\$RUN_TESTS" =~ ^[Yy]$ ]]; then
  echo "🧪 Running tests..."
  pytest -q --disable-warnings || echo "⚠️ Some tests failed."
fi

echo "✅ VM QuickOps completed successfully at \$(date -u)"
EOF

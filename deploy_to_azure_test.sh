#!/usr/bin/env bash
# ==========================================================
# Azure Test Deployment Script — Unguided Capstone (Step 8)
# Supports: --fast flag to skip dependency installation
# Includes: .env sync + remote variable export
# ==========================================================

set -euo pipefail

FAST_MODE=false
if [[ "${1:-}" == "--fast" ]]; then
  FAST_MODE=true
  echo "⚡ Running in FAST mode — skipping dependency installation."
fi

echo "📦 Loading environment variables..."
source .env

# ---- Guard Clauses ----
: "${AZURE_APP_ID:?Missing AZURE_APP_ID in .env}"
: "${AZURE_APP_SECRET:?Missing AZURE_APP_SECRET in .env}"
: "${AZURE_TENANT_ID:?Missing AZURE_TENANT_ID in .env}"
: "${AZURE_RESOURCE_GROUP:?Missing AZURE_RESOURCE_GROUP in .env}"
: "${AZURE_VM_NAME:?Missing AZURE_VM_NAME in .env}"

echo "🔐 Logging in to Azure..."
az login --service-principal \
  -u "$AZURE_APP_ID" \
  -p "$AZURE_APP_SECRET" \
  --tenant "$AZURE_TENANT_ID" >/dev/null

echo "🧠 Checking VM status..."
VM_STATE=$(az vm get-instance-view \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$AZURE_VM_NAME" \
  --query "instanceView.statuses[1].code" -o tsv)

if [[ "$VM_STATE" != "PowerState/running" ]]; then
  echo "▶️ Starting VM..."
  az vm start --resource-group "$AZURE_RESOURCE_GROUP" --name "$AZURE_VM_NAME" >/dev/null
else
  echo "✅ VM already running."
fi

VM_IP=$(az vm show -d \
  --resource-group "$AZURE_RESOURCE_GROUP" \
  --name "$AZURE_VM_NAME" \
  --query publicIps -o tsv)

echo "🌐 VM IP: $VM_IP"

# ------------------------------------------------------------
# Push .env to VM so Spark and pytest see it
# ------------------------------------------------------------
echo "📤 Copying .env to VM..."
scp -o StrictHostKeyChecking=no .env azureuser@"$VM_IP":/home/azureuser/.env >/dev/null

echo "⚙️ Running pipeline on VM..."

# ------------------------------------------------------------
# Remote execution block
# ------------------------------------------------------------
ssh -o StrictHostKeyChecking=no azureuser@"$VM_IP" <<EOF
set -e
echo "💡 Remote session started on \$(hostname)"
source ~/.bashrc || true

# Load environment variables from synced .env
if [ -f ~/.env ]; then
  export \$(grep -v '^#' ~/.env | xargs)
  echo "✅ Environment variables loaded from .env"
fi

cd /home/azureuser
rm -rf unguided-capstone-project
git clone --branch step8-dev https://github.com/mtholahan/unguided-capstone-project.git
cd unguided-capstone-project

mkdir -p test_results

if [ "$FAST_MODE" = false ]; then
  echo "📦 Installing dependencies..."
  sudo apt-get update -qq
  sudo apt-get install -y python3-pip >/dev/null
  pip3 install --quiet --upgrade pip
  if [ -f requirements_stable.txt ]; then
      pip3 install --quiet -r requirements_stable.txt
  elif [ -f requirements_locked.txt ]; then
      pip3 install --quiet -r requirements_locked.txt
  else
      pip3 install --quiet pandas pyspark pytest
  fi
  pip3 install --quiet rapidfuzz || echo "rapidfuzz already satisfied"
else
  echo "⚡ Skipping dependency installation (FAST mode)."
fi

echo "🔍 Verifying environment modules..."
python3 scripts/verify_env.py || exit 1

echo "🚀 Running Spark job..."
if command -v spark-submit >/dev/null 2>&1; then
    spark-submit --master local[2] scripts/main.py > test_results/pipeline_run.log 2>&1 \
      || echo "Spark job failed (non-zero exit code)" >> test_results/pipeline_run.log
else
    echo "spark-submit not found in PATH" > test_results/pipeline_run.log
fi

echo "🧪 Running pytest..."
pytest -v > test_results/pytest_report.log 2>&1 || true

tar -czf /home/azureuser/test_results.tgz -C test_results .
echo "📦 test_results.tgz created."
EOF

# ------------------------------------------------------------
# Retrieve and show summary
# ------------------------------------------------------------
echo "⬇️ Copying results back..."
mkdir -p ~/test_results
scp -o StrictHostKeyChecking=no azureuser@"$VM_IP":/home/azureuser/test_results.tgz ~/test_results/ >/dev/null
tar -xzf ~/test_results/test_results.tgz -C ~/test_results

echo ""
echo "✅ Results available locally in ~/test_results"
echo "📜 --- pipeline_run.log (last 20 lines) ---"
tail -20 ~/test_results/pipeline_run.log 2>/dev/null || echo "No pipeline_run.log found"
echo ""
echo "📜 --- pytest_report.log (last 20 lines) ---"
tail -20 ~/test_results/pytest_report.log 2>/dev/null || echo "No pytest_report.log found"

echo ""
echo "🧩 Log summary complete."
echo "🧠 Tip: Use '--fast' for redeploys once VM dependencies are set up."

# ------------------------------------------------------------
# Optional: Stop VM after completion (disabled for testing)
# ------------------------------------------------------------
# echo "🛑 Stopping VM to avoid Azure charges..."
# az vm deallocate --resource-group "$AZURE_RESOURCE_GROUP" --name "$AZURE_VM_NAME"
# echo "☁️ VM successfully stopped."

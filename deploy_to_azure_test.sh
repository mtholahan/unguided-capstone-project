#!/usr/bin/env bash
# ===========================================
# Step 8 – Deploy TMDB–Discogs Pipeline to Azure VM for Testing
# Author: Springboard Data Bootcamp Coach
# ===========================================

set -e  # stop on error

# ---- LOAD ENVIRONMENT ----
if [ -f ".env" ]; then
  echo "📦 Loading environment variables from .env..."
  export $(grep -v '^#' .env | xargs)
else
  echo "⚠️  .env file not found! Ensure AZURE_APP_ID, AZURE_APP_SECRET, and AZURE_TENANT_ID are set."
fi

# ---- AUTHENTICATE (Service Principal on Local Machine) ----
echo "🔐 Authenticating to Azure using Service Principal..."
az login --service-principal \
  -u "$AZURE_APP_ID" \
  -p "$AZURE_APP_SECRET" \
  --tenant "$AZURE_TENANT_ID" >/dev/null

# ---- CONFIG ----
RESOURCE_GROUP="rg-unguidedcapstone-test"
VM_NAME="ungcapvm01"
STORAGE_ACCOUNT="ungcapstor01"
CONTAINER_NAME="testresults"
REPO_URL="https://github.com/mtholahan/unguided-capstone-project.git"
WORK_DIR="/home/azureuser/unguided-capstone-project"
RESULTS_DIR="$WORK_DIR/test_results"
BLOB_PATH="https://${STORAGE_ACCOUNT}.blob.core.windows.net/${CONTAINER_NAME}"
KEYVAULT_NAME="ungcapkv01"

# ---- VM PREP ----
echo "🧠 Checking VM status..."
VM_STATE=$(az vm get-instance-view -g $RESOURCE_GROUP -n $VM_NAME --query "instanceView.statuses[1].code" -o tsv)
if [[ "$VM_STATE" != "PowerState/running" ]]; then
  echo "🚀 Starting VM..."
  az vm start -g $RESOURCE_GROUP -n $VM_NAME >/dev/null
else
  echo "✅ VM already running."
fi

VM_IP=$(az vm show -d -g $RESOURCE_GROUP -n $VM_NAME --query publicIps -o tsv)
echo "🌐 VM IP: $VM_IP"

# ---- DEPLOY CODE ----
echo "📦 Syncing GitHub repo..."
ssh -o StrictHostKeyChecking=no azureuser@$VM_IP "rm -rf $WORK_DIR && git clone $REPO_URL $WORK_DIR"

# ---- RUN PIPELINE ----
echo "⚙️ Executing Spark pipeline..."
ssh azureuser@$VM_IP <<'EOF'
cd ~/unguided-capstone-project || exit 1
mkdir -p test_results
# Optional: activate venv if exists
[ -d "venv" ] && source venv/bin/activate
if command -v spark-submit &>/dev/null; then
  spark-submit main.py > test_results/pipeline_run.log 2>&1
else
  echo "⚠️ spark-submit not found; skipping pipeline execution." > test_results/pipeline_run.log
fi
EOF

# ---- RUN TESTS ----
echo "🧪 Running PyTest suite (if present)..."
ssh azureuser@$VM_IP <<'EOF'
cd ~/unguided-capstone-project || exit 1
if [ -d "tests" ]; then
  pytest -v --maxfail=1 --disable-warnings > test_results/pytest_report.log 2>&1
else
  echo "⚠️ No tests directory yet – skipping pytest." > test_results/pytest_report.log
fi
EOF

# ---- UPLOAD RESULTS ----
echo "☁️ Uploading results to Azure Blob..."
ssh azureuser@$VM_IP <<EOF
export PATH=\$PATH:/usr/bin
if ! command -v az &>/dev/null; then
  echo "📦 Installing Azure CLI (first-time setup)..."
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
fi

echo "🔐 Logging in with VM Managed Identity..."
az login --identity >/dev/null

echo "☁️ Uploading test results with Managed Identity..."
az storage container create \
  --account-name $STORAGE_ACCOUNT \
  --name $CONTAINER_NAME \
  --auth-mode login \
  --public-access off >/dev/null

az storage blob upload-batch \
  --account-name $STORAGE_ACCOUNT \
  --auth-mode login \
  -s ~/unguided-capstone-project/test_results \
  -d $CONTAINER_NAME >/dev/null
EOF


# ---- TEARDOWN ----
echo "💤 Deallocating VM..."
az vm deallocate -g $RESOURCE_GROUP -n $VM_NAME >/dev/null

echo "🎯 Deployment & test cycle complete."
echo "Logs uploaded to: $BLOB_PATH"

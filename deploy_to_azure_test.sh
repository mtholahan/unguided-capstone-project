#!/usr/bin/env bash
# ===========================================
# Step 8 – Deploy TMDB–Discogs Pipeline to Azure VM for Testing
# ===========================================

set -e

# ---- LOAD ENVIRONMENT ----
if [ -f ".env" ]; then
  echo "📦 Loading environment variables from .env..."
  export $(grep -v '^#' .env | xargs)
else
  echo "⚠️ .env file not found! Ensure AZURE_APP_ID, AZURE_APP_SECRET, and AZURE_TENANT_ID are set."
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

# ---- DEPLOY CODE & RUN PIPELINE REMOTELY ----
ssh azureuser@$VM_IP bash <<'EOF'
set -e
echo "📦 Syncing GitHub repo..."
rm -rf unguided-capstone-project
git clone --branch step8-dev git@github.com:mtholahan/unguided-capstone-project.git
cd unguided-capstone-project
echo "🌿 Branch: $(git rev-parse --abbrev-ref HEAD)"

echo "⚙️ Running Spark pipeline..."
mkdir -p test_results
if command -v spark-submit &>/dev/null; then
  spark-submit main.py > test_results/pipeline_run.log 2>&1
else
  echo "⚠️ spark-submit not found." > test_results/pipeline_run.log
fi

echo "🧪 Running PyTest..."
if [ -d "tests" ]; then
  pytest -v --maxfail=1 --disable-warnings > test_results/pytest_report.log 2>&1
else
  echo "⚠️ No tests directory." > test_results/pytest_report.log
fi

echo "📤 Compressing results..."
tar -czf /tmp/test_results.tgz -C test_results .
EOF

# ---- RETRIEVE RESULTS TO LOCAL TEMP FOLDER ----
LOCAL_RESULTS="$HOME/test_results"
mkdir -p "$LOCAL_RESULTS"
echo "⬇️ Copying test artifacts from VM..."
scp azureuser@$VM_IP:/tmp/test_results.tgz "$LOCAL_RESULTS/"
tar -xzf "$LOCAL_RESULTS/test_results.tgz" -C "$LOCAL_RESULTS"
rm "$LOCAL_RESULTS/test_results.tgz"

# ---- UPLOAD TO BLOB STORAGE (LOCAL SP AUTH) ----
echo "☁️ Uploading test results via Service Principal..."

# 🔑  Get an account key (fast, synchronous)
ACCOUNT_KEY=$(az storage account keys list \
  --account-name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query "[0].value" -o tsv)

# Create the container if it doesn't exist
az storage container create \
  --name "$CONTAINER_NAME" \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$ACCOUNT_KEY" \
  --public-access off >/dev/null

# Upload results using the key (never token-based)
az storage blob upload-batch \
  --destination "$CONTAINER_NAME" \
  --account-name "$STORAGE_ACCOUNT" \
  --account-key "$ACCOUNT_KEY" \
  --source "$LOCAL_RESULTS" \
  --overwrite >/dev/null


echo "✅ Upload complete → $BLOB_PATH"

# ---- TEARDOWN ----
echo "💤 Deallocating VM..."
az vm deallocate -g $RESOURCE_GROUP -n $VM_NAME >/dev/null
echo "🎯 Deployment & test cycle complete."

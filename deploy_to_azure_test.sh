#!/usr/bin/env bash
# ===========================================
# Step 8 – Deploy TMDB–Discogs Pipeline to Azure VM for Testing
# Author: Springboard Data Bootcamp Coach
# ===========================================

# ---- CONFIG ----
RESOURCE_GROUP="rg-unguidedcapstone-test"
VM_NAME="ungcapvm01"
STORAGE_ACCOUNT="ungcapstor01"
CONTAINER_NAME="testresults"
REPO_URL="git@github.com:mtholahan/unguided-capstone-project.git"
WORK_DIR="/home/azureuser/unguided-capstone-project"
RESULTS_DIR="$WORK_DIR/test_results"
BLOB_PATH="https://${STORAGE_ACCOUNT}.blob.core.windows.net/${CONTAINER_NAME}"
KEYVAULT_NAME="ungcapkv01"

# ---- PREP ----
echo "🔐 Logging in to Azure..."
az account show >/dev/null 2>&1 || az login

echo "🧠 Checking VM status..."
VM_STATE=$(az vm get-instance-view -g $RESOURCE_GROUP -n $VM_NAME --query "instanceView.statuses[1].code" -o tsv)

if [[ "$VM_STATE" != "PowerState/running" ]]; then
  echo "🚀 Starting VM..."
  az vm start -g $RESOURCE_GROUP -n $VM_NAME
else
  echo "✅ VM already running."
fi

# ---- CONNECT ----
VM_IP=$(az vm show -d -g $RESOURCE_GROUP -n $VM_NAME --query publicIps -o tsv)
echo "🌐 VM IP: $VM_IP"

# ---- DEPLOY CODE ----
echo "📦 Syncing GitHub repo..."
ssh azureuser@$VM_IP "rm -rf $WORK_DIR && git clone $REPO_URL $WORK_DIR"

# ---- RUN PIPELINE ----
echo "⚙️ Executing Spark pipeline..."
ssh azureuser@$VM_IP <<'EOF'
cd ~/unguided-capstone-project
# Activate environment
source venv/bin/activate 2>/dev/null || true
mkdir -p test_results
spark-submit main.py > test_results/pipeline_run.log 2>&1
EOF

# ---- RUN TESTS ----
echo "🧪 Running PyTest suite (if present)..."
ssh azureuser@$VM_IP <<'EOF'
cd ~/unguided-capstone-project
if [ -d "tests" ]; then
    pytest -v --maxfail=1 --disable-warnings > test_results/pytest_report.log 2>&1
else
    echo "⚠️ No tests directory yet – skipping pytest." > test_results/pytest_report.log
fi
EOF

# ---- UPLOAD RESULTS ----
echo "☁️ Uploading results to Azure Blob..."
ssh azureuser@$VM_IP <<EOF
az storage container create --account-name $STORAGE_ACCOUNT --name $CONTAINER_NAME --public-access off
az storage blob upload-batch --account-name $STORAGE_ACCOUNT -s ~/unguided-capstone-project/test_results -d $CONTAINER_NAME
EOF

# ---- TEARDOWN ----
echo "💤 Deallocating VM..."
az vm deallocate -g $RESOURCE_GROUP -n $VM_NAME

echo "🎯 Deployment & test cycle complete."
echo "Logs uploaded to: $BLOB_PATH"

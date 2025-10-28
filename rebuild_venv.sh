#!/usr/bin/env bash
# =====================================================================
# rebuild_venv.sh – Step 8/9 Environment Bootstrapper (Root-level)
# ---------------------------------------------------------------------
# Purpose:
#   Rebuilds or refreshes your PySpark + Azure dev environment locally,
#   and optionally exports updated requirements to your Azure VM.
# =====================================================================

set -euo pipefail

# -------------------------------------------------------
# Path & config setup
# -------------------------------------------------------
PROJECT_ROOT="$(pwd)"
VENV_PATH="$HOME/pyspark_venv311"
REQ_STABLE="$PROJECT_ROOT/requirements_stable.txt"
REQ_LOCKED="$PROJECT_ROOT/requirements_locked.txt"

FORCE_REBUILD=false
EXPORT_VM=false

# -------------------------------------------------------
# Parse arguments
# -------------------------------------------------------
for arg in "$@"; do
  case "$arg" in
    --force) FORCE_REBUILD=true ;;
    --export) EXPORT_VM=true ;;
  esac
done

# -------------------------------------------------------
# Create / rebuild virtual environment
# -------------------------------------------------------
if [[ "$FORCE_REBUILD" == true ]]; then
  echo "⚠️  Force rebuild requested — deleting old venv..."
  rm -rf "$VENV_PATH"
fi

if [[ ! -d "$VENV_PATH" ]]; then
  echo "🧱 Creating new virtual environment at $VENV_PATH..."
  python3 -m venv "$VENV_PATH"
else
  echo "♻️  Reusing existing virtual environment at $VENV_PATH..."
fi

# Activate the virtual environment
# shellcheck disable=SC1091
source "$VENV_PATH/bin/activate"

echo ""
echo "✅ Virtual environment activated: $VIRTUAL_ENV"
echo ""

# -------------------------------------------------------
# Install core dependencies
# -------------------------------------------------------
echo "📦 Installing dependencies into virtual environment..."
pip install --upgrade pip setuptools wheel

# Install project runtime dependencies
pip install pyspark==3.5.2 pandas requests rapidfuzz tqdm pyarrow fastparquet
pip install azure-identity azure-storage-blob python-dotenv
pip install pytest pytest-cov flake8 black
pip install matplotlib seaborn

# -------------------------------------------------------
# Install build-time tools (pipreqs)
# -------------------------------------------------------
echo "🛠️  Installing build-time tools (pipreqs)..."
pip install pipreqs >/dev/null 2>&1 || {
  echo "⚠️  pipreqs installation failed — skipping dependency inventory refresh"
}

# -------------------------------------------------------
# Rebuild dependency inventories
# -------------------------------------------------------
if command -v pipreqs >/dev/null 2>&1; then
  echo "📋 Generating dependency inventories..."
  pip freeze > "$REQ_LOCKED"
  pipreqs . --force --savepath "$REQ_STABLE" --ignore pyspark_venv311 --no-follow-links
  echo "📦 Dependency inventories refreshed (stable & locked)."
else
  echo "⚠️  pipreqs not found; skipping inventory generation."
fi

# -------------------------------------------------------
# Environment summary
# -------------------------------------------------------
echo ""
echo "✅ Environment rebuilt successfully!"
echo " - Virtual environment: $VENV_PATH"
echo " - requirements_stable.txt: $REQ_STABLE"
echo " - requirements_locked.txt: $REQ_LOCKED"
echo ""
echo "👉 To activate manually later: source $VENV_PATH/bin/activate"

# -------------------------------------------------------
# Optional: Export requirements to Azure VM
# -------------------------------------------------------
if [[ "$EXPORT_VM" == true ]]; then
  echo ""
  echo "☁️  Exporting requirements to Azure VM..."

  if [[ ! -f ".env" ]]; then
    echo "❌ .env not found — cannot export to VM."
    exit 1
  fi

  source .env
  : "${AZURE_APP_ID:?Missing AZURE_APP_ID in .env}"
  : "${AZURE_APP_SECRET:?Missing AZURE_APP_SECRET in .env}"
  : "${AZURE_TENANT_ID:?Missing AZURE_TENANT_ID in .env}"
  : "${AZURE_RESOURCE_GROUP:?Missing AZURE_RESOURCE_GROUP in .env}"
  : "${AZURE_VM_NAME:?Missing AZURE_VM_NAME in .env}"

  echo "🔐 Logging into Azure..."
  az login --service-principal \
    -u "$AZURE_APP_ID" -p "$AZURE_APP_SECRET" --tenant "$AZURE_TENANT_ID" >/dev/null

  VM_IP=$(az vm show -d --resource-group "$AZURE_RESOURCE_GROUP" --name "$AZURE_VM_NAME" --query publicIps -o tsv)

  if [[ -z "$VM_IP" ]]; then
    echo "❌ Could not resolve VM IP. Is the VM running?"
    exit 1
  fi

  echo "🌐 VM IP: $VM_IP"
  echo "📤 Copying requirement files to VM..."
  scp -o StrictHostKeyChecking=no "$REQ_STABLE" "$REQ_LOCKED" azureuser@"$VM_IP":/home/azureuser/ >/dev/null
  echo "✅ requirements_stable.txt and requirements_locked.txt exported successfully."
fi

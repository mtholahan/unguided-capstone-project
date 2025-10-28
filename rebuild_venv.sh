#!/usr/bin/env bash
# =====================================================================
# rebuild_venv.sh – Step 8/9 Environment Bootstrapper (Root-level)
# ---------------------------------------------------------------------
# Purpose:
#   Rebuilds or refreshes your PySpark + Azure dev environment locally,
#   and optionally exports updated requirements to your Azure VM.
#
# Usage:
#   bash rebuild_venv.sh          # rebuild or reuse venv locally
#   bash rebuild_venv.sh --force  # destroy and rebuild venv
#   bash rebuild_venv.sh --export # push requirements to Azure VM
#
# Notes:
#   - Writes requirements_stable.txt and requirements_locked.txt
#     to the project root.
#   - Mirrors deploy_to_azure_test.sh’s authentication pattern.
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
    --force)
      FORCE_REBUILD=true
      ;;
    --export)
      EXPORT_VM=true
      ;;
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

# shellcheck disable=SC1091
source "$VENV_PATH/bin/activate"

# -------------------------------------------------------
# Install dependencies
# -------------------------------------------------------
echo "📦 Installing dependencies into virtual environment..."
pip install --upgrade pip setuptools wheel

# Core data + Spark stack
pip install pyspark==3.5.2 pandas requests rapidfuzz tqdm pyarrow fastparquet

# Azure + environment management
pip install azure-identity azure-storage-blob python-dotenv

# Testing + quality
pip install pytest pytest-cov flake8 black

# Visualization + analysis
pip install matplotlib seaborn

# -------------------------------------------------------
# Freeze environment
# -------------------------------------------------------
echo "📝 Writing requirement snapshots to project root..."
pip freeze > "$REQ_LOCKED"

# Stable (short list of core libs)
pip list --format=freeze | grep -E '^(pyspark|pandas|requests|rapidfuzz|tqdm|pyarrow|pytest|python-dotenv|azure|flake8|black|matplotlib|seaborn)' > "$REQ_STABLE"

echo ""
echo "✅ Environment rebuilt successfully!"
echo " - Virtual environment: $VENV_PATH"
echo " - requirements_stable.txt: $REQ_STABLE"
echo " - requirements_locked.txt: $REQ_LOCKED"

# -------------------------------------------------------
# Optional: Export requirements to Azure VM
# -------------------------------------------------------
if [[ "$EXPORT_VM" == true ]]; then
  echo ""
  echo "☁️  Exporting requirements to Azure VM..."

  # Ensure .env is present
  if [[ ! -f ".env" ]]; then
    echo "❌ .env not found — cannot export to VM."
    exit 1
  fi

  # Load environment variables
  source .env

  : "${AZURE_APP_ID:?Missing AZURE_APP_ID in .env}"
  : "${AZURE_APP_SECRET:?Missing AZURE_APP_SECRET in .env}"
  : "${AZURE_TENANT_ID:?Missing AZURE_TENANT_ID in .env}"
  : "${AZURE_RESOURCE_GROUP:?Missing AZURE_RESOURCE_GROUP in .env}"
  : "${AZURE_VM_NAME:?Missing AZURE_VM_NAME in .env}"

  echo "🔐 Logging into Azure..."
  az login --service-principal \
    -u "$AZURE_APP_ID" \
    -p "$AZURE_APP_SECRET" \
    --tenant "$AZURE_TENANT_ID" >/dev/null

  VM_IP=$(az vm show -d \
    --resource-group "$AZURE_RESOURCE_GROUP" \
    --name "$AZURE_VM_NAME" \
    --query publicIps -o tsv)

  if [[ -z "$VM_IP" ]]; then
    echo "❌ Could not resolve VM IP. Is the VM running?"
    exit 1
  fi

  echo "🌐 VM IP: $VM_IP"
  echo "📤 Copying requirement files to VM..."
  scp -o StrictHostKeyChecking=no "$REQ_STABLE" "$REQ_LOCKED" azureuser@"$VM_IP":/home/azureuser/ >/dev/null
  echo "✅ requirements_stable.txt and requirements_locked.txt exported successfully."
fi

# -------------------------------------------------------
# Post-rebuild verification (optional DRY closure)
# -------------------------------------------------------
CHECK_SCRIPT="$PROJECT_ROOT/check_env.sh"
SYNC_LOG="$PROJECT_ROOT/sync_log.md"

if [[ -x "$CHECK_SCRIPT" ]]; then
  echo ""
  echo "🔎 Running post-rebuild environment check..."
  if bash "$CHECK_SCRIPT"; then
    echo "✅ Environment validation succeeded."
    # Append to sync log
    echo "| $(date -u '+%Y-%m-%d %H:%M:%S') | $(hostname) | post-rebuild check_env | ✅ passed |" >> "$SYNC_LOG"
  else
    echo "❌ Environment validation failed after rebuild."
    echo "| $(date -u '+%Y-%m-%d %H:%M:%S') | $(hostname) | post-rebuild check_env | ❌ failed |" >> "$SYNC_LOG"
    exit 1
  fi
else
  echo "⚠️  check_env.sh not found or not executable — skipping post-rebuild check."
  echo "| $(date -u '+%Y-%m-%d %H:%M:%S') | $(hostname) | post-rebuild check_env | ⚠️ skipped |" >> "$SYNC_LOG"
fi


echo ""
echo "👉 To activate locally: source $VENV_PATH/bin/activate"

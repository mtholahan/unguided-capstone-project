#!/usr/bin/env bash
# =====================================================================
# rebuild_venv.sh  –  Step 8 Environment Bootstrapper
# ---------------------------------------------------------------------
# Purpose:
#   Rebuilds or verifies the PySpark virtual environment for
#   the Unguided Capstone project (local + Databricks parity).
#
# Behavior:
#   - Creates ~/pyspark_venv311  (Python 3.11.x recommended)
#   - Installs Spark + Azure + testing dependencies
#   - Writes requirements_stable.txt and requirements_locked.txt
#
# Usage:
#   bash scripts/rebuild_venv.sh          # normal run (verify or reuse)
#   bash scripts/rebuild_venv.sh --force  # destroy and rebuild from scratch
# =====================================================================

set -e

VENV_PATH="$HOME/pyspark_venv311"
REQ_STABLE="requirements_stable.txt"
REQ_LOCKED="requirements_locked.txt"
FORCE_REBUILD=false

# -------------------------------------------------------
# Parse args
# -------------------------------------------------------
if [[ "$1" == "--force" ]]; then
    FORCE_REBUILD=true
    echo "⚠️  Force rebuild requested – existing venv will be removed."
fi

# -------------------------------------------------------
# Create / Recreate venv
# -------------------------------------------------------
if [[ "$FORCE_REBUILD" == true ]]; then
    rm -rf "$VENV_PATH"
fi

if [[ ! -d "$VENV_PATH" ]]; then
    echo "🧱 Creating new virtual environment at $VENV_PATH..."
    python3 -m venv "$VENV_PATH"
else
    echo "♻️  Reusing existing virtual environment at $VENV_PATH..."
fi

# Activate
# shellcheck disable=SC1091
source "$VENV_PATH/bin/activate"

# -------------------------------------------------------
# Core installs
# -------------------------------------------------------
echo "🔧 Upgrading pip and build tools..."
pip install --upgrade pip setuptools wheel

echo "📦 Installing PySpark core + utilities..."
pip install pyspark==3.5.2 pandas requests rapidfuzz

# -------------------------------------------------------
# Step 8 additions – testing + Azure + dotenv
# -------------------------------------------------------
echo "🧩 Installing Step 8 dependencies (pytest, dotenv, Azure libs)..."
pip install pytest pytest-cov python-dotenv
pip install azure-identity azure-storage-blob

# -------------------------------------------------------
# Freeze environments
# -------------------------------------------------------
echo "📝 Writing requirement snapshots..."
pip freeze > "$REQ_LOCKED"
pip list --format=freeze | grep -E '^(pyspark|pytest|python-dotenv|azure|rapidfuzz|pandas)' > "$REQ_STABLE"

# -------------------------------------------------------
# Completion summary
# -------------------------------------------------------
echo "✅ Done."
echo " - Virtual environment: $VENV_PATH"
echo " - Stable requirements: $REQ_STABLE"
echo " - Locked snapshot:     $REQ_LOCKED"
echo " - Activate with:  source $VENV_PATH/bin/activate"

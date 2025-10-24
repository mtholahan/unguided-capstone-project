#!/usr/bin/env bash
# =====================================================================
# rebuild_venv.sh  –  Step 8/9 Environment Bootstrapper
# ---------------------------------------------------------------------
# Purpose:
#   Rebuilds or verifies the PySpark virtual environment for
#   the Unguided Capstone (local + Databricks parity).
#
# Notes:
#   - Organized into dependency blocks for maintainability.
#   - Core, Azure, Testing, and Extension (optional) modules included.
#
# Usage:
#   bash scripts/rebuild_venv.sh          # verify or reuse existing venv
#   bash scripts/rebuild_venv.sh --force  # destroy and rebuild from scratch
# =====================================================================

set -e

VENV_PATH="$HOME/pyspark_venv311"
REQ_STABLE="$(pwd)/requirements_stable.txt"
REQ_LOCKED="$(pwd)/requirements_locked.txt"
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
# Core dependencies
# -------------------------------------------------------
echo "📦 Installing core PySpark + data libraries..."
pip install --upgrade pip setuptools wheel
pip install pyspark==3.5.2 pandas requests rapidfuzz tqdm pyarrow

# -------------------------------------------------------
# Azure + Environment Management
# -------------------------------------------------------
echo "🔐 Installing Azure + dotenv dependencies..."
pip install azure-identity azure-storage-blob python-dotenv

# -------------------------------------------------------
# Testing + Quality
# -------------------------------------------------------
echo "🧪 Installing testing and linting dependencies..."
pip install pytest pytest-cov flake8 black

# -------------------------------------------------------
# Optional: Data Engineering / Scaling Extensions
#   (uncomment as needed for Step 9+)
# -------------------------------------------------------
# echo "⚙️ Installing extended Spark / Delta / ML utilities..."
# pip install delta-spark==3.2.0 mlflow boto3

# -------------------------------------------------------
# Optional: Visualization / Reporting
#   (safe to skip in production clusters)
# -------------------------------------------------------
# pip install matplotlib seaborn notebook jupyterlab

# -------------------------------------------------------
# Freeze environments
# -------------------------------------------------------
echo "📝 Writing requirement snapshots..."
pip freeze > "$REQ_LOCKED"
pip list --format=freeze | grep -E '^(pyspark|pandas|requests|rapidfuzz|tqdm|pyarrow|pytest|python-dotenv|azure|flake8|black)' > "$REQ_STABLE"

# -------------------------------------------------------
# Completion summary
# -------------------------------------------------------
echo "✅ Done."
echo " - Virtual environment: $VENV_PATH"
echo " - Stable requirements: $REQ_STABLE"
echo " - Locked snapshot:     $REQ_LOCKED"
echo " - Activate with:  source $VENV_PATH/bin/activate"

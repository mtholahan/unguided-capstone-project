#!/bin/bash
# ======================================================
# Rebuild & Export Stable PySpark Environment
# Context: Ubuntu WSL2 | Python 3.11 | TMDB Capstone
# Author: Mark | Updated: Oct 2025
# ======================================================

set -e

VENV_PATH="$HOME/pyspark_venv311"
REQ_FILE="$(pwd)/requirements_stable.txt"
LOCK_FILE="$(pwd)/requirements_locked.txt"
FORCE_REBUILD=false

# Parse optional flags
for arg in "$@"; do
  case $arg in
    --force)
      FORCE_REBUILD=true
      shift
      ;;
  esac
done

echo "======================================================"
echo " TMDB Capstone | Environment Setup Utility"
echo "======================================================"
echo ""

# ------------------------------------------------------
# 1. Handle venv creation or reuse
# ------------------------------------------------------
if [ -d "$VENV_PATH" ] && [ "$FORCE_REBUILD" = false ]; then
  echo "✅ Virtual environment already exists at: $VENV_PATH"
  echo "👉 Skipping rebuild (use --force to recreate)."
  source "$VENV_PATH/bin/activate"
else
  if [ "$FORCE_REBUILD" = true ]; then
    echo "⚠️  Force rebuild requested. Removing existing venv..."
    rm -rf "$VENV_PATH"
  fi

  echo ">>> Creating new venv at $VENV_PATH ..."
  python3.11 -m venv "$VENV_PATH"
  source "$VENV_PATH/bin/activate"

  echo ">>> Upgrading pip and setuptools..."
  pip install --upgrade pip wheel setuptools

  echo ">>> Installing stable dependencies..."
  pip install \
    pyspark==3.5.2 \
    pandas==2.0.3 \
    pyarrow==13.0.0 \
    numpy==1.26.4 \
    matplotlib==3.8.4 \
    rapidfuzz==3.9.0 \
    azure-core==1.36.0 \
    azure-storage-blob==12.20.0 \
    boto3==1.35.0 \
    python-dotenv==1.0.1 \
    ipykernel

  echo ">>> Verifying installs..."
  python - <<'EOF'
import sys, pandas, pyspark, numpy
print(f"\nPython: {sys.version.split()[0]}")
print(f"Pandas: {pandas.__version__}")
print(f"PySpark: {pyspark.__version__}")
print(f"Numpy: {numpy.__version__}\n")
EOF
fi

# ------------------------------------------------------
# 2. Refresh stable requirements (short list)
# ------------------------------------------------------
echo ">>> Generating clean requirements_stable.txt ..."
pip freeze --exclude-editable | grep -E 'pyspark|pandas|pyarrow|numpy|matplotlib|rapidfuzz|azure|boto3|dotenv|ipykernel' > "$REQ_FILE"

# ------------------------------------------------------
# 3. Export full lock file (complete freeze)
# ------------------------------------------------------
echo ">>> Generating full requirements_locked.txt ..."
pip freeze > "$LOCK_FILE"

# ------------------------------------------------------
# 4. Wrap up
# ------------------------------------------------------
echo ""
echo "✅ Done."
echo " - Virtual environment: $VENV_PATH"
echo " - Stable requirements file: $REQ_FILE"
echo " - Locked snapshot file: $LOCK_FILE"
echo ""
echo "To activate later: source $VENV_PATH/bin/activate"
echo "Use './rebuild_venv.sh --force' to recreate the venv from scratch."
echo "======================================================"

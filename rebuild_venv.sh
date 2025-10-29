#!/usr/bin/env bash
# =====================================================================
# rebuild_venv.sh – Step 8/9 Environment Bootstrapper (Refactored)
# ---------------------------------------------------------------------
# Aligns PySpark with local Spark (3.5.3) and Java 17
# =====================================================================

set -euo pipefail

PROJECT_ROOT="$(pwd)"
VENV_PATH="$HOME/pyspark_venv311"
REQ_STABLE="$PROJECT_ROOT/requirements_stable.txt"
REQ_LOCKED="$PROJECT_ROOT/requirements_locked.txt"

SPARK_HOME_DEFAULT="/home/mark/spark"
JAVA_HOME_DEFAULT="/usr/lib/jvm/java-17-openjdk-amd64"

FORCE_REBUILD=false
EXPORT_VM=false

for arg in "$@"; do
  case "$arg" in
    --force)  FORCE_REBUILD=true ;;
    --export) EXPORT_VM=true ;;
  esac
done

# -------------------------------------------------------
# Virtual environment creation
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
echo "✅ Virtual environment activated: $VIRTUAL_ENV"
echo ""

# -------------------------------------------------------
# Core dependency install
# -------------------------------------------------------
echo "📦 Installing dependencies..."
pip install --upgrade pip setuptools wheel

# Match local Spark binary version exactly
pip install pyspark==3.5.3 pandas requests rapidfuzz tqdm pyarrow fastparquet \
  --break-system-packages

pip install azure-identity azure-storage-blob python-dotenv \
            pytest pytest-cov flake8 black matplotlib seaborn \
  --break-system-packages

# -------------------------------------------------------
# Environment variable alignment
# -------------------------------------------------------
echo "⚙️  Ensuring Spark + Java environment variables..."

export SPARK_HOME="${SPARK_HOME:-$SPARK_HOME_DEFAULT}"
export JAVA_HOME="${JAVA_HOME:-$JAVA_HOME_DEFAULT}"
export PATH="$SPARK_HOME/bin:$PATH"

mkdir -p "$VENV_PATH/bin/activate.d"

cat > "$VENV_PATH/bin/activate.d/env_spark.sh" <<EOF
# Auto-added by rebuild_venv.sh
export SPARK_HOME="$SPARK_HOME"
export JAVA_HOME="$JAVA_HOME"
export PATH="\$SPARK_HOME/bin:\$PATH"
EOF

echo " - SPARK_HOME=$SPARK_HOME"
echo " - JAVA_HOME=$JAVA_HOME"
echo ""

# -------------------------------------------------------
# Dependency inventory
# -------------------------------------------------------
echo "📋 Generating dependency inventories..."
pip freeze > "$REQ_LOCKED"
if pip show pipreqs >/dev/null 2>&1; then
  pipreqs . --force --savepath "$REQ_STABLE" --ignore pyspark_venv311 --no-follow-links || true
else
  pip install pipreqs --break-system-packages && \
  pipreqs . --force --savepath "$REQ_STABLE" --ignore pyspark_venv311 --no-follow-links
fi

# -------------------------------------------------------
# Post-build Spark sanity test
# -------------------------------------------------------
echo ""
echo "🧪 Verifying SparkSession creation..."
python3 - <<'PYCODE'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SanityCheck").getOrCreate()
print(f"✅ Spark initialized: {spark.version}")
spark.stop()
PYCODE

echo ""
echo "✅ Environment rebuilt successfully!"
echo "👉 To activate later: source $VENV_PATH/bin/activate"

# -------------------------------------------------------
# Optional export to Azure VM (unchanged)
# -------------------------------------------------------
if [[ "$EXPORT_VM" == true ]]; then
  echo ""
  echo "☁️  Exporting requirement files to Azure VM..."
  source .env
  : "${AZURE_APP_ID:?Missing AZURE_APP_ID}"
  : "${AZURE_APP_SECRET:?Missing AZURE_APP_SECRET}"
  : "${AZURE_TENANT_ID:?Missing AZURE_TENANT_ID}"
  : "${AZURE_RESOURCE_GROUP:?Missing AZURE_RESOURCE_GROUP}"
  : "${AZURE_VM_NAME:?Missing AZURE_VM_NAME}"

  az login --service-principal -u "$AZURE_APP_ID" -p "$AZURE_APP_SECRET" --tenant "$AZURE_TENANT_ID" >/dev/null
  VM_IP=$(az vm show -d --resource-group "$AZURE_RESOURCE_GROUP" --name "$AZURE_VM_NAME" --query publicIps -o tsv)
  scp -o StrictHostKeyChecking=no "$REQ_STABLE" "$REQ_LOCKED" azureuser@"$VM_IP":/home/azureuser/ >/dev/null
  echo "✅ Requirements exported successfully."
fi

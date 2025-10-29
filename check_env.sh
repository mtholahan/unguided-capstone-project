#!/usr/bin/env bash
# =====================================================================
# check_env.sh — Capstone Environment Pre-Flight Diagnostic
# ---------------------------------------------------------------------
# Purpose:
#   Verifies Spark, Python, venv, and Azure prerequisites before running
#   run_pipeline_safe.sh. Exits non-zero if misconfiguration detected.
# =====================================================================

set -euo pipefail
source "$HOME/pyspark_venv311/bin/activate" 2>/dev/null || true
echo "🔍 Running Capstone environment check..."

# -------------------------------------------------------
# 1️⃣ Python & virtualenv
# -------------------------------------------------------
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  echo "⚠️  No virtualenv active — please run: source pyspark_venv311/bin/activate"
  exit 1
fi
python3 --version

# -------------------------------------------------------
# 2️⃣ Spark autodetection
# -------------------------------------------------------
if [[ -z "${SPARK_HOME:-}" ]]; then
  # Try common Spark install locations
  for path in \
      "/opt/spark" \
      "$HOME/spark" \
      "$HOME/spark-3.5.3-bin-hadoop3" \
      "/usr/local/spark" \
      "/usr/lib/spark"; do
    if [[ -d "$path" && -x "$path/bin/spark-submit" ]]; then
      export SPARK_HOME="$path"
      break
    fi
  done
fi

if [[ -z "${SPARK_HOME:-}" ]]; then
  echo "❌ SPARK_HOME not set and no Spark installation found."
  echo "   Please install Spark or export SPARK_HOME manually."
  exit 1
fi

echo "✅ SPARK_HOME=${SPARK_HOME}"

if ! command -v "$SPARK_HOME/bin/spark-submit" >/dev/null 2>&1; then
  echo "❌ spark-submit not found under $SPARK_HOME/bin"
  exit 1
fi

# Add Spark to PATH for downstream scripts
export PATH="$SPARK_HOME/bin:$PATH"

# -------------------------------------------------------
# 3️⃣ Core directories
# -------------------------------------------------------
for d in data logs; do
  [[ -d "$d" ]] || { echo "⚠️  Directory '$d' missing — creating..."; mkdir -p "$d"; }
done

# -------------------------------------------------------
# 4️⃣ Critical dependencies
# -------------------------------------------------------
echo "🧩 Checking critical Python packages..."
# python -m rapidfuzz --version >/dev/null 2>&1 || { echo "❌ rapidfuzz not installed"; exit 1; }
if python -c "import rapidfuzz" >/dev/null 2>&1; then
  echo "✅ rapidfuzz detected"
else
  echo "❌ rapidfuzz not installed"
  exit 1
fi
python -c "import pyspark, dotenv; print('✅ pyspark', pyspark.__version__)" || exit 1

# -------------------------------------------------------
# 5️⃣ Git context & version tag
# -------------------------------------------------------
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "📦 Git tag: $(git describe --always --tags 2>/dev/null || echo 'un-tagged')"
else
  echo "⚠️  Not a git repository"
fi

echo "✅ Environment check passed. Safe to run pipeline."

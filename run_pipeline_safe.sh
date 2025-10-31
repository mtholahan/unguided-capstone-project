#!/usr/bin/env bash
# =====================================================================
# run_pipeline_safe.sh — Unified pipeline launcher (Definitive Version)
# ---------------------------------------------------------------------
# Ensures the environment is validated, Spark is configured,
# logs are captured, and the orchestrator runs safely end-to-end.
# =====================================================================

set -euo pipefail

# --- Source environment variables ---
if [ -f ~/.bashrc ]; then
  source ~/.bashrc
fi

# --- Pre-flight validation ---
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
bash "$SCRIPT_DIR/check_env.sh" || {
  echo "❌ Environment validation failed. Aborting run."
  exit 1
}

# --- Root and Spark setup ---
export PIPELINE_ROOT="$HOME/Projects/unguided-capstone-project"
export SPARK_HOME="${SPARK_HOME:-$HOME/spark}"
export PATH="$SPARK_HOME/bin:$PATH"

# --- Directories (define early to avoid undefined vars) ---
export LOG_DIR="$PIPELINE_ROOT/logs"
export PIPELINE_OUTPUT_DIR="$PIPELINE_ROOT/data/intermediate"
export PIPELINE_METRICS_DIR="$PIPELINE_ROOT/data/metrics"
mkdir -p "$LOG_DIR" "$PIPELINE_OUTPUT_DIR" "$PIPELINE_METRICS_DIR"

# --- Logging setup ---
LOGFILE="$LOG_DIR/pipeline_run_$(date +%Y%m%d_%H%M%S).log"
touch "$LOGFILE"

# --- Python setup ---
PYTHON_PATH="$(which python3 || which python || true)"
if [[ -z "$PYTHON_PATH" ]]; then
  echo "❌ No Python interpreter found. Aborting run." | tee -a "$LOGFILE"
  exit 1
fi
export PYSPARK_PYTHON="$PYTHON_PATH"
export PYSPARK_DRIVER_PYTHON="$PYTHON_PATH"

# --- Log initial configuration ---
echo "🐍 Using Python interpreter: $PYTHON_PATH" | tee -a "$LOGFILE"
echo "🚀 Running pipeline with Spark at $(date)" | tee -a "$LOGFILE"
echo "🧩 Using Spark at: $SPARK_HOME" | tee -a "$LOGFILE"
echo "📄 Logfile: $LOGFILE" | tee -a "$LOGFILE"

# --- Move to project root ---
cd "$PIPELINE_ROOT"

# --- Sanity check: confirm orchestrator exists ---
PIPELINE_ENTRY="$PIPELINE_ROOT/scripts/main.py"
if [[ ! -f "$PIPELINE_ENTRY" ]]; then
  echo "❌ Orchestrator not found at $PIPELINE_ENTRY" | tee -a "$LOGFILE"
  exit 1
fi

start_time=$(date +%s)

# --- Run the orchestrator with live output and logging ---
{
  "$SPARK_HOME/bin/spark-submit" \
    --master local[*] \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$PIPELINE_ROOT/conf/log4j.properties" \
    "$PIPELINE_ENTRY"
} 2>&1 | tee -a "$LOGFILE"


# --- Completion timing ---
end_time=$(date +%s)
elapsed=$(( end_time - ${start_time:-0} ))

if (( elapsed < 5 )); then
  echo "⚠️  Warning: Pipeline completed in ${elapsed}s — check for premature exit or skipped steps." | tee -a "$LOGFILE"
else
  echo "✅ Pipeline finished successfully in ${elapsed}s at $(date)" | tee -a "$LOGFILE"
fi

echo "📊 Output metrics (if generated): $PIPELINE_METRICS_DIR" | tee -a "$LOGFILE"


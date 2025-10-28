#!/bin/bash
set -e
source ~/.bashrc

# --- Pre-flight validation ---
bash "$(dirname "$0")/check_env.sh" || {
  echo "❌ Environment validation failed. Aborting run."
  exit 1
}

# --- Setup Spark PATH safely ---
export SPARK_HOME="$HOME/spark-3.5.3-bin-hadoop3"
export PATH="$SPARK_HOME/bin:$PATH"

# --- Core pipeline paths ---
export PIPELINE_ROOT="$HOME/unguided-capstone-project"
export PIPELINE_OUTPUT_DIR="$PIPELINE_ROOT/data/intermediate"
export PIPELINE_METRICS_DIR="$PIPELINE_ROOT/data/metrics"

# --- Ensure directories exist ---
mkdir -p "$PIPELINE_OUTPUT_DIR" "$PIPELINE_METRICS_DIR" "$PIPELINE_ROOT/logs"

# --- Run the pipeline ---
cd "$PIPELINE_ROOT"
LOGFILE="$PIPELINE_ROOT/logs/pipeline_run_$(date +%Y%m%d_%H%M%S).log"
echo "🚀 Running pipeline with Spark at $(date)" | tee -a "$LOGFILE"

# --- Ensure Spark and Python are visible ---
export SPARK_HOME=${SPARK_HOME:-$PIPELINE_ROOT/spark-3.5.3-bin-hadoop3}
export PATH="$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-$PIPELINE_ROOT/pyspark_venv311/bin/python}
export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON

spark-submit --version >> "$LOGFILE" 2>&1
spark-submit --master local[4] pipeline_main.py >> "$LOGFILE" 2>&1

echo "✅ Pipeline finished at $(date)" | tee -a "$LOGFILE"

#!/bin/bash
set -e
source ~/.bashrc

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

spark-submit --version >> "$LOGFILE" 2>&1
spark-submit --master local[4] pipeline_main.py >> "$LOGFILE" 2>&1

echo "✅ Pipeline finished at $(date)" | tee -a "$LOGFILE"

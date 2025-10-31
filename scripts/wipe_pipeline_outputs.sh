#!/usr/bin/env bash
# =====================================================================
# wipe_pipeline_outputs.sh
# Full data reset + post-clean status summary for Unguided Capstone
# ---------------------------------------------------------------------
# Deletes intermediate outputs, metrics, validation, cache, and raw data.
# Logs all actions to data/logs/cleanup.log.
# Includes a --dry-run flag for safe simulation.
# =====================================================================

set -euo pipefail

BASE="$HOME/Projects/unguided-capstone-project/data"
TARGETS=(
  "$BASE/intermediate"
  "$BASE/metrics"
  "$BASE/validation"
  "$BASE/cache"
  "$BASE/raw/discogs_raw"
  "$BASE/raw/tmdb_raw"
)
LOG_DIR="$BASE/logs"
LOG_PATH="$LOG_DIR/cleanup.log"

# -------------------------------------------------------
# CLI options
# -------------------------------------------------------
DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
  echo "⚠️  DRY RUN mode: No files will be deleted."
fi

# -------------------------------------------------------
# Ensure log directory exists
# -------------------------------------------------------
mkdir -p "$LOG_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting FULL pipeline cleanup..." | tee -a "$LOG_PATH"

# -------------------------------------------------------
# Cleanup loop
# -------------------------------------------------------
for dir in "${TARGETS[@]}"; do
  if [ -d "$dir" ]; then
    echo "[$(date '+%H:%M:%S')] Wiping contents of $dir" | tee -a "$LOG_PATH"
    if [ "$DRY_RUN" = false ]; then
      find "$dir" -mindepth 1 -delete 2>/dev/null || true
    fi
  else
    echo "[$(date '+%H:%M:%S')] Creating missing directory: $dir" | tee -a "$LOG_PATH"
    if [ "$DRY_RUN" = false ]; then
      mkdir -p "$dir"
    fi
  fi
done

# -------------------------------------------------------
# Post-clean status summary
# -------------------------------------------------------
echo "------------------------------------------------------------" | tee -a "$LOG_PATH"
echo "📊 Directory Status Summary:" | tee -a "$LOG_PATH"

for dir in "${TARGETS[@]}"; do
  if [ -d "$dir" ]; then
    FILE_COUNT=$(find "$dir" -type f | wc -l)
    DIR_SIZE=$(du -sh "$dir" 2>/dev/null | awk '{print $1}')
    echo "   • $(basename "$dir") → $FILE_COUNT files | $DIR_SIZE" | tee -a "$LOG_PATH"
  else
    echo "   • $(basename "$dir") → MISSING ❌" | tee -a "$LOG_PATH"
  fi
done

echo "------------------------------------------------------------" | tee -a "$LOG_PATH"

# -------------------------------------------------------
# Completion
# -------------------------------------------------------
if [ "$DRY_RUN" = true ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ DRY RUN complete. No files were deleted." | tee -a "$LOG_PATH"
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Cleanup complete. All directories reset." | tee -a "$LOG_PATH"
fi

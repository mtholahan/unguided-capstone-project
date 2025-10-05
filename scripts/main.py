# ============================================================
# main.py  — Pipeline Orchestrator
# ============================================================
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime
import pandas as pd

# ---- Step Imports ----
from step_00_acquire_musicbrainz import Step00AcquireMusicbrainz
from step_01_audit_raw import Step01AuditRaw
from step_02_cleanse_tsv import Step02CleanseTSV
from step_03_util_check_tsv_structure import Step03CheckStructure
from step_03b_rehydrate_guids import Step03BRehydrateGuids
from step_04_mb_full_join import Step04MBFullJoin
from step_05_filter_soundtracks_enhanced import Step05FilterSoundtracksEnhanced
from step_06_fetch_tmdb import Step06FetchTMDb
from step_07_prepare_tmdb_input import Step07PrepareTMDbInput
from step_08_match_tmdb import Step08MatchTMDb
from step_09_apply_rescues import Step09ApplyRescues
from step_10_enrich_tmdb import Step10EnrichMatches
from step_10b_coverage_audit import Step10BCoverageAudit

from config import DATA_DIR, TMDB_DIR, STEP_METRICS

# ============================================================
# 🪵 Global Logging Configuration
# ============================================================
LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(exist_ok=True)
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"pipeline_run_{timestamp}.log"

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ],
)

logger = logging.getLogger("Pipeline")
logger.info(f"📜 Logging initialized → {log_file}")

# Track runtimes
STEP_TIMES = {}

# ============================================================
# Step Orchestration
# ============================================================
def build_steps():
    return [
        Step00AcquireMusicbrainz(cleanup_archives=False),
        Step01AuditRaw(),
        Step02CleanseTSV(),
        Step03CheckStructure(),
        Step03BRehydrateGuids(),
        Step04MBFullJoin(),
        Step05FilterSoundtracksEnhanced(),
        Step06FetchTMDb(),
        Step07PrepareTMDbInput(),
        Step08MatchTMDb(),
        Step09ApplyRescues(),
        Step10EnrichMatches(),
        Step10BCoverageAudit(),
    ]


def safe_count(path: Path) -> str:
    """Return row count of CSV/TSV file, or '-' if not available."""
    if not path.exists() or path.stat().st_size == 0:
        return "-"
    try:
        if path.suffix == ".tsv":
            pd.read_csv(path, sep="\t", nrows=5)
        else:
            pd.read_csv(path, nrows=5)
        with path.open(encoding="utf-8", errors="ignore") as f:
            total = sum(1 for _ in f) - 1
        return f"{total:,}"
    except Exception as e:
        return f"ERR ({e})"


def print_summary(steps):
    """Log + write pipeline summary to file."""
    summary_files = {
        "Step 04 output": DATA_DIR / "joined_release_data.tsv",
        "Step 05 output": DATA_DIR / "soundtracks.tsv",
        "Step 06 output": TMDB_DIR / "enriched_top_1000.csv",
        "Step 07 output": TMDB_DIR / "tmdb_input_candidates_clean.csv",
        "Step 08 output": TMDB_DIR / "tmdb_match_results.csv",
        "Step 09 output": TMDB_DIR / "tmdb_match_results_enhanced.csv",
        "Step 10 output": TMDB_DIR / "tmdb_enriched_matches.csv",
        "Step 10B output (audit)": TMDB_DIR / "coverage_audit.csv",
        "Step 10B output (summary)": TMDB_DIR / "coverage_summary.txt",
    }

    lines = ["📊 Pipeline Summary"]
    for step in steps:
        step_num = step.name.split(":")[0].split()[-1].zfill(2)
        label = f"{step.name}"
        runtime = STEP_TIMES.get(step_num, None)

        count = "-"
        for lbl, path in summary_files.items():
            if lbl.startswith(f"Step {step_num}"):
                count = safe_count(path)
                break

        if runtime is not None:
            lines.append(f"   {label:<35} {count} rows   ⏱ {runtime:.1f}s")
        else:
            lines.append(f"   {label:<35} {count} rows")

    if "golden_fidelity" in STEP_METRICS:
        g = STEP_METRICS
        lines.append("")
        lines.append(
            f"⭐ Golden Test Fidelity: {g['golden_matched']}/{g['golden_total']} "
            f"({g['golden_fidelity']:.1f}%)"
        )

    for line in lines:
        logger.info(line)

    summary_file = Path("Pipeline_Summary.txt")
    with summary_file.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    logger.info(f"📝 Pipeline summary written to {summary_file.resolve()}")


# ============================================================
# Main Execution Logic
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Run the Movie Soundtrack Pipeline")
    parser.add_argument("--resume", type=str, default=None,
                        help="Step number to resume from (e.g., '05' to start at Step05FilterSoundtracks)")
    args = parser.parse_args()

    steps = build_steps()
    start_index = 0

    if args.resume:
        target = args.resume.strip().upper().replace("STEP", "").replace(":", "")
        matched = False
        for i, step in enumerate(steps):
            step_id = step.name.split(":")[0].split()[-1].upper().replace("STEP", "").replace(":", "")
            if step_id == target or step_id.zfill(2) == target.zfill(2):
                start_index = i
                matched = True
                logger.info(f"▶ Resuming pipeline at {step.name}")
                break
        if not matched:
            valid_steps = [s.name.split(':')[0].split()[-1].upper() for s in steps]
            logger.error(f"❌ Invalid resume step: {args.resume}")
            logger.error(f"   Valid step IDs: {', '.join(valid_steps)}")
            return

    for step in steps[start_index:]:
        logger.info(f"▶ Running {step.name}...")
        start_time = time.time()
        try:
            step.run()
            elapsed = time.time() - start_time
            step_num = step.name.split(":")[0].split()[-1]
            STEP_TIMES[step_num] = elapsed
            logger.info(f"✅ {step.name} complete. ⏱ {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - start_time
            step_num = step.name.split(":")[0].split()[-1]
            STEP_TIMES[step_num] = elapsed
            logger.error(f"❌ {step.name} failed after {elapsed:.1f}s: {e}", exc_info=True)
            break

    print_summary(steps)
    print(f"\n✅ Pipeline run complete. Full log saved to: {log_file}")


if __name__ == "__main__":
    main()

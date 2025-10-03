# main.py
import logging
import argparse

from step_00_acquire_musicbrainz import Step00AcquireMusicbrainz
from step_01_audit_raw import Step01AuditRaw
from step_02_cleanse_tsv import Step02CleanseTSV
from step_03_util_check_tsv_structure import Step03CheckStructure
from step_04_mb_full_join import Step04MBFullJoin
from step_05_filter_soundtracks import Step05FilterSoundtracks
from step_06_fetch_tmdb import Step06FetchTMDb
from step_07_prepare_tmdb_input import Step07PrepareTMDbInput
from step_08_match_tmdb import Step08MatchTMDb
from step_09_apply_rescues import Step09ApplyRescues
from step_10_enrich_tmdb import Step10EnrichMatches

# Use the root logger configured in base_step.py
logger = logging.getLogger(__name__)


def build_steps():
    """Return the ordered list of pipeline steps."""
    return [
        Step00AcquireMusicbrainz(cleanup_archives=False),
        Step01AuditRaw(),
        Step02CleanseTSV(),
        Step03CheckStructure(),
        Step04MBFullJoin(),
        Step05FilterSoundtracks(),
        Step06FetchTMDb(),
        Step07PrepareTMDbInput(),
        Step08MatchTMDb(),
        Step09ApplyRescues(),
        Step10EnrichMatches(),
    ]


def main():
    parser = argparse.ArgumentParser(description="Run the Movie Soundtrack Pipeline")
    parser.add_argument(
        "--resume",
        type=str,
        default=None,
        help="Step number to resume from (e.g. '05' to start at Step05FilterSoundtracks)",
    )
    args = parser.parse_args()

    steps = build_steps()

    # Map step number strings ("00", "01", "05", etc.) to index
    start_index = 0
    if args.resume:
        for i, step in enumerate(steps):
            step_num = step.name.split(":")[0].split()[-1]  # e.g. "Step 05"
            step_num = step_num.zfill(2)
            if step_num == args.resume.zfill(2):
                start_index = i
                logger.info(f"▶ Resuming pipeline at {step.name}")
                break
        else:
            logger.error(f"❌ Invalid resume step: {args.resume}")
            return

    # Run pipeline from chosen step
    for step in steps[start_index:]:
        logger.info(f"▶ Running {step.name}...")
        try:
            step.run()
            logger.info(f"✅ {step.name} complete.")
        except Exception as e:
            logger.error(f"❌ {step.name} failed: {e}", exc_info=True)
            break


if __name__ == "__main__":
    main()

# main.py
import logging

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


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("pipeline.log", mode="w"),
            logging.StreamHandler()
        ]
    )


def main():
    setup_logging()
    logger = logging.getLogger("Pipeline")

    steps = [
        Step00AcquireMusicbrainz(),
        Step01AuditRaw(),
        Step02CleanseTSV(),
        Step03CheckStructure(),
        Step04MBFullJoin(),
        Step05FilterSoundtracks(),
        Step06FetchTMDb(),
        Step07PrepareTMDbInput(),
        Step08MatchTMDb(),
        Step09ApplyRescues(),
        Step10EnrichMatches()
    ]

    for step in steps:
        logger.info(f"▶ Running {step.name}...")
        try:
            step.run()
            logger.info(f"✅ {step.name} complete.")
        except Exception as e:
            logger.error(f"❌ {step.name} failed: {e}")
            break


if __name__ == "__main__":
    main()

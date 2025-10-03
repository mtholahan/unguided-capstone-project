# base_step.py

import logging
from pathlib import Path
import datetime
import sys
from tqdm import tqdm
from config import DEBUG_MODE  # Global debug toggle


class BaseStep:
    def __init__(self, name: str, config: dict = None):
        self.name = name
        self.config = config or {}
        self.timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.setup_logger()

    def fail(self, msg: str):
        self.logger.error(msg)
        raise RuntimeError(msg)

    def setup_logger(self):
        """
        Configure logging once per pipeline run.
        - Console (UTF-8 safe)
        - Global pipeline.log (appends all steps)
        """
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # Ensure stdout supports UTF-8
        except AttributeError:
            # Fallback for Python < 3.7
            import io
            sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding="utf-8")

        log_dir = Path(self.config.get("log_dir", "./logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "pipeline.log"  # single unified log

        # Root logger (only configured once)
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                handlers=[
                    logging.FileHandler(log_file, mode="w", encoding="utf-8"),
                    logging.StreamHandler(sys.stdout),
                ],
            )

        self.logger = logging.getLogger(self.name)
        self.logger.info(f"Initialized step: {self.name} (DEBUG_MODE={DEBUG_MODE})")

    def progress_iter(self, iterable, **kwargs):
        """Wrapper for tqdm with default description set to step name."""
        if "desc" not in kwargs:
            kwargs["desc"] = self.name
        return tqdm(iterable, **kwargs)

    def run(self):
        raise NotImplementedError("Subclasses must implement a run() method.")

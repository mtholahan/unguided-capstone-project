import logging
import json
import datetime
import io
from pathlib import Path

def get_logger(name: str, log_dir: str = "data/logs"):
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.FileHandler(Path(log_dir) / f"{name}.log")
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def write_checkpoint(step: str, status: str):
    Path("data/metrics").mkdir(parents=True, exist_ok=True)
    payload = {
        "step": step,
        "status": status,
        "timestamp": datetime.datetime.now().isoformat(),
    }
    path = Path("data/metrics/pipeline_checkpoints.json")
    import io
    with io.open(path, "a", encoding="utf-8", buffering=1) as f:
        json.dump(payload, f)
        f.write("\n")


def write_metrics(metrics: dict, name: str, metrics_dir: str = "data/metrics"):
    """
    Write per-step metrics JSON file.
    """
    metrics_path = Path(metrics_dir) / f"{name}.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

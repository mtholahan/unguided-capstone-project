# tests/test_utils_package.py
import os, json
from pathlib import Path
from scripts.utils import io_utils, env, logging as log, schema
from scripts.utils.base_step import BaseStep
from scripts.utils.base_extractor import BaseExtractor
import pandas as pd

# --- Test I/O functions ---
def test_atomic_write_and_safe_json(tmp_path):
    path = tmp_path / "test.json"
    data = {"a": 1, "b": 2}
    io_utils.safe_json_dump(data, path)
    loaded = io_utils.safe_json_load(path)
    assert loaded == data

def test_atomic_write_overwrite(tmp_path):
    path = tmp_path / "overwrite.txt"
    io_utils.atomic_write(path, "old")
    io_utils.atomic_write(path, "new")
    assert path.read_text() == "new"

# --- Test env loading ---
def test_load_env(monkeypatch):
    monkeypatch.setenv("PIPELINE_ROOT", "/tmp")
    env_vars = env.load_env()
    assert Path(env_vars["root"]).exists()
    assert "api_keys" in env_vars

# --- Test logging and checkpointing ---
def test_logger_and_checkpoint(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    logger = log.get_logger("test_step")
    logger.info("Testing logger.")
    log.write_checkpoint("StepTest", "success")
    cp = Path("data/metrics/pipeline_checkpoints.json")
    assert cp.exists()
    content = cp.read_text().strip().split("\n")[-1]
    entry = json.loads(content)
    assert entry["status"] == "success"

# --- Test BaseStep ---
class DummyStep(BaseStep):
    def run(self):
        Path("run_marker.txt").write_text("done")

def test_base_step_run_with_checkpoint(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    step = DummyStep("Dummy")
    step.run_with_checkpoint()
    assert Path("run_marker.txt").exists()
    cp = Path("data/metrics/pipeline_checkpoints.json")
    assert "Dummy" in cp.read_text()

# --- Test BaseExtractor ---
class DummyExtractor(BaseExtractor):
    def run(self):
        pass

def test_base_extractor_init():
    e = DummyExtractor("TestSource", "https://example.com")
    assert e.base_url == "https://example.com"
    assert e.name.startswith("Extract_")

# --- Test Schema utilities ---
def test_infer_schema_and_key_health():
    df = pd.DataFrame({"id": [1, 2, 2], "val": [10, 20, None]})
    sch = schema.infer_schema(df)
    assert "id" in sch
    metrics = schema.key_health(df, ["id"])
    assert metrics["duplicates"] == 1

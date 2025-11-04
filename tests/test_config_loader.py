import os
import json
import pytest

# adjust the import path if your loader is in scripts/config_loader.py
from scripts.config_loader import load_config


def test_config_loader_valid(monkeypatch, tmp_path):
    """Verifies that config file loads correctly and includes required keys."""
    fake_cfg = {
        "adls_key": "abc123",
        "input_path": "/mnt/input",
        "output_path": "/mnt/output"
    }
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps(fake_cfg))

    monkeypatch.setenv("CONFIG_PATH", str(cfg_file))

    conf = load_config()
    assert isinstance(conf, dict)
    assert conf["adls_key"] == "abc123"
    assert set(["adls_key", "input_path", "output_path"]).issubset(conf.keys())


def test_config_loader_missing_key(monkeypatch, tmp_path):
    """Raises KeyError when essential keys are missing."""
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({"input_path": "/mnt/input"}))
    monkeypatch.setenv("CONFIG_PATH", str(cfg_file))

    with pytest.raises(KeyError):
        load_config()

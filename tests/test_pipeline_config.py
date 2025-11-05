import os

def test_config_imports(app_config):
    assert app_config is not None  # config.py loads without Databricks

def test_local_dirs_defined(app_config):
    assert hasattr(app_config, "DATA_DIR")
    assert hasattr(app_config, "LOCAL_PATHS")
    assert "raw" in app_config.LOCAL_PATHS

def test_adls_strings_present(app_config):
    assert hasattr(app_config, "RAW_DIR")
    assert isinstance(app_config.RAW_DIR, str)
    assert app_config.RAW_DIR.startswith("abfss://")

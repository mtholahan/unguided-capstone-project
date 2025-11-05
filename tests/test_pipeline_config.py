import os

def test_config_paths_exist(app_config):
    assert hasattr(app_config, "DATA_PATH")
    assert os.path.exists(app_config.DATA_PATH)

def test_env_vars_present():
    for var in ["AZURE_STORAGE_KEY", "BLOB_CONTAINER"]:
        assert var in os.environ, f"{var} not set"

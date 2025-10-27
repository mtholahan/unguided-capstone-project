"""
config_env.py
-------------------------------------------------------
Loads and validates environment variables from `.env`,
and injects Azure credentials into Spark configuration.
-------------------------------------------------------
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession


REQUIRED_VARS = [
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_STORAGE_KEY",
    "AZURE_STORAGE_CONTAINER",
]

def ensure_local_env_defaults():
    """
    Injects default Azure-like environment variables if not present.
    This prevents verify_env.py and local Spark sessions from failing
    when running outside of Azure or CI.
    """
    defaults = {
        "AZURE_STORAGE_ACCOUNT": "localstorage",
        "AZURE_STORAGE_KEY": "localkey",
        "AZURE_STORAGE_CONTAINER": "localcontainer",
        "LOCAL_MODE": "true",
    }

    injected = []
    for key, value in defaults.items():
        if not os.getenv(key):
            os.environ[key] = value
            injected.append(key)

    if injected:
        print(f"⚙️ Injected local defaults for: {', '.join(injected)}")
    else:
        print("🌐 All required Azure environment variables already set.")


def load_and_validate_env():
    """Load .env and verify required keys."""
    load_dotenv()  # Reads from project root
    missing = [k for k in REQUIRED_VARS if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing required environment vars: {missing}")
    return {k: os.getenv(k) for k in REQUIRED_VARS}


def configure_spark_from_env(spark: SparkSession):
    """Inject Azure Storage credentials into Spark session."""
    env = load_and_validate_env()
    account = env["AZURE_STORAGE_ACCOUNT"]
    key = env["AZURE_STORAGE_KEY"]
    spark.conf.set(f"fs.azure.account.key.{account}.dfs.core.windows.net", key)
    print(f"✅ Spark configured for Azure account: {account}")


def show_env_summary():
    """Simple summary (safe for console logs)."""
    print("🔧 Active environment configuration:")
    for k, v in load_and_validate_env().items():
        masked = v[:4] + "..." if v else "None"
        print(f"  {k} = {masked}")


if __name__ == "__main__":
    # Manual smoke test
    load_and_validate_env()
    show_env_summary()

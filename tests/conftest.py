import pytest
import os, sys
from pyspark.sql import SparkSession

# Ensure project root is on path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import scripts.config as config

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("CapstoneTestSuite")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def app_config():
    return config

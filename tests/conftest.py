import pytest
from pyspark.sql import SparkSession
from config import Config

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
    return Config()

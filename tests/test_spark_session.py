import pytest
from pyspark.sql import SparkSession

# adjust the import if your init function lives elsewhere
from scripts.spark_init import init_spark


@pytest.fixture(scope="module")
def spark():
    spark = init_spark(app_name="test_app")
    yield spark
    spark.stop()


def test_spark_session_initialized(spark):
    """Confirms Spark session starts successfully with correct app name."""
    assert isinstance(spark, SparkSession)
    assert "test_app" in spark.sparkContext.appName


def test_spark_conf_contains_adls_keys(spark):
    """Ensures Spark configuration includes ADLS credentials."""
    conf_items = spark.sparkContext.getConf().getAll()
    key_entries = [k for k, _ in conf_items if "fs.azure.account.key" in k]
    assert key_entries, "Expected at least one ADLS key in Spark configuration"

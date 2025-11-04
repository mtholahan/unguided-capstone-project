import pytest
from scripts.pipeline_runner import Pipeline_Runner  # adjust import path
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("pipeline_test").getOrCreate()
    yield spark
    spark.stop()

def test_pipeline_dryrun(tmp_path, spark):
    # Mock small input dataset
    df = spark.createDataFrame(
        [(1, "A", 10.0), (2, "B", 20.0)],
        ["id", "category", "value"]
    )
    input_path = str(tmp_path / "input.parquet")
    df.write.parquet(input_path)

    output_path = str(tmp_path / "output")

    # Run pipeline dry-run
    runner = Pipeline_Runner(mode="test", input_path=input_path, output_path=output_path)
    result = runner.run()

    assert result["status"] == "success"
    assert (tmp_path / "metrics").exists() or (tmp_path / "output").exists()

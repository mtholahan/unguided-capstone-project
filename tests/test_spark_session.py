def test_spark_session_starts(spark):
    assert spark is not None
    df = spark.createDataFrame([(1, "ok")], ["id", "status"])
    assert df.count() == 1
    assert "status" in df.columns

def test_spark_session_starts(spark):
    assert spark is not None
    df = spark.createDataFrame([(1, "ok")], ["id", "status"])
    assert df.count() == 1
    assert "status" in df.columns

# --- Edge Cases ---

def test_empty_dataframe_creation(spark):
    df = spark.createDataFrame([], "id INT, status STRING")
    assert df.count() == 0


def test_invalid_path_key_in_config(app_config):
    import pytest
    with pytest.raises(KeyError):
        _ = app_config.LOCAL_PATHS["nonexistent"]

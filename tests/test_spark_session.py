def test_spark_session_starts(spark):
    assert spark is not None
    df = spark.createDataFrame([(1, "ok")], ["id", "status"])
    assert df.count() == 1
    assert "status" in df.columns

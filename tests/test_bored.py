"""Tests for BoredDataSource."""

import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import BoredDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_bored_registration(spark):
    spark.dataSource.register(BoredDataSource)
    assert BoredDataSource.name() == "bored"

def test_bored_read(spark):
    spark.dataSource.register(BoredDataSource)
    try:
        df = spark.read.format("bored").option("limit", "3").load()
        assert "activity" in df.columns
        if df.count() == 0:
            pytest.skip("Bored API returned no data")
    except Exception as e:
        if "timeout" in str(e).lower():
            pytest.skip("Network unavailable")
        raise

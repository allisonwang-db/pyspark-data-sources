"""Tests for QuotableDataSource."""

import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import QuotableDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_quotable_registration(spark):
    spark.dataSource.register(QuotableDataSource)
    assert QuotableDataSource.name() == "quotable"

def test_quotable_read(spark):
    spark.dataSource.register(QuotableDataSource)
    try:
        df = spark.read.format("quotable").option("limit", "5").load()
        assert "content" in df.columns
        if df.count() == 0:
            pytest.skip("Quotable API returned no data")
    except Exception as e:
        if "timeout" in str(e).lower():
            pytest.skip("Network unavailable")
        raise

"""Tests for HttpbinDataSource."""

import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import HttpbinDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_httpbin_registration(spark):
    spark.dataSource.register(HttpbinDataSource)
    assert HttpbinDataSource.name() == "httpbin"

def test_httpbin_read(spark):
    spark.dataSource.register(HttpbinDataSource)
    try:
        df = spark.read.format("httpbin").load()
        assert "origin" in df.columns
        if df.count() == 0:
            pytest.skip("HTTPBin API returned no data")
    except Exception as e:
        if "timeout" in str(e).lower():
            pytest.skip("Network unavailable")
        raise

"""Tests for IpApiDataSource."""

import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import IpApiDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_ipapi_registration(spark):
    spark.dataSource.register(IpApiDataSource)
    assert IpApiDataSource.name() == "ipapi"

def test_ipapi_read(spark):
    spark.dataSource.register(IpApiDataSource)
    try:
        df = spark.read.format("ipapi").load()
        assert df.count() == 1
    except Exception as e:
        if "timeout" in str(e).lower():
            pytest.skip("Network unavailable")
        raise

"""Tests for UniversitiesDataSource."""

import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import UniversitiesDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_universities_registration(spark):
    spark.dataSource.register(UniversitiesDataSource)
    assert UniversitiesDataSource.name() == "universities"

def test_universities_read(spark):
    spark.dataSource.register(UniversitiesDataSource)
    try:
        df = spark.read.format("universities").load()
        assert "name" in df.columns
        if df.count() == 0:
            pytest.skip("Universities API returned no data")
    except Exception as e:
        if "timeout" in str(e).lower():
            pytest.skip("Network unavailable")
        raise

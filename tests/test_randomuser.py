"""Tests for RandomUserDataSource."""

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import RandomUserDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_randomuser_datasource_registration(spark):
    """Test that RandomUserDataSource can be registered."""
    spark.dataSource.register(RandomUserDataSource)
    assert RandomUserDataSource.name() == "randomuser"


def test_randomuser_read(spark):
    """Test reading random users (integration test - uses real API)."""
    spark.dataSource.register(RandomUserDataSource)
    try:
        df = spark.read.format("randomuser").option("results", "5").load()
        rows = df.collect()
        assert len(rows) <= 5
        assert len(rows) > 0
        assert "first_name" in df.columns
        assert "email" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower() or "connection" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

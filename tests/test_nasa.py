"""Tests for NasaDataSource."""

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import NasaDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_nasa_datasource_registration(spark):
    """Test that NasaDataSource can be registered."""
    spark.dataSource.register(NasaDataSource)
    assert NasaDataSource.name() == "nasa"


def test_nasa_requires_api_key(spark):
    """Test that api_key is required."""
    spark.dataSource.register(NasaDataSource)
    with pytest.raises((ValueError, Exception), match="api_key"):
        spark.read.format("nasa").load().collect()


def test_nasa_read(spark):
    """Test reading APOD (integration test - uses real API with DEMO_KEY)."""
    spark.dataSource.register(NasaDataSource)
    try:
        df = spark.read.format("nasa").option("api_key", "DEMO_KEY").option("count", "3").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "title" in df.columns
        assert "date" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower() or "429" in str(exc) or "rate" in str(exc).lower():
            pytest.skip("NASA API rate limit or network unavailable")
        raise

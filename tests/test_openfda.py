"""Tests for OpenFdaDataSource."""

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import OpenFdaDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_openfda_datasource_registration(spark):
    """Test that OpenFdaDataSource can be registered."""
    spark.dataSource.register(OpenFdaDataSource)
    assert OpenFdaDataSource.name() == "openfda"


def test_openfda_read(spark):
    """Test reading drug labels (integration test - uses real API)."""
    spark.dataSource.register(OpenFdaDataSource)
    try:
        df = spark.read.format("openfda").option("limit", "5").load()
        rows = df.collect()
        assert len(rows) > 0
        assert len(rows) <= 5
        assert "brand_name" in df.columns
        assert "purpose" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower() or "connection" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

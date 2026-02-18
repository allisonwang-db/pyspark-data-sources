"""Tests for PyPiDataSource."""

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import PyPiDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_pypi_datasource_registration(spark):
    """Test that PyPiDataSource can be registered."""
    spark.dataSource.register(PyPiDataSource)
    assert PyPiDataSource.name() == "pypi"


def test_pypi_read(spark):
    """Test reading PyPI package (integration - uses real API)."""
    spark.dataSource.register(PyPiDataSource)
    try:
        df = spark.read.format("pypi").load("requests")
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["package_name"] == "requests"
        assert rows[0]["version"] != ""
        assert "summary" in df.columns
    except Exception as exc:
        msg = str(exc).lower()
        if "timeout" in msg or "connection" in msg:
            pytest.skip("PyPI API unavailable")
        raise

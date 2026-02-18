"""Tests for RssDataSource."""

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import RssDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_rss_datasource_registration(spark):
    """Test that RssDataSource can be registered."""
    spark.dataSource.register(RssDataSource)
    assert RssDataSource.name() == "rss"


def test_rss_read(spark):
    """Test reading RSS feed (integration - uses real API)."""
    spark.dataSource.register(RssDataSource)
    try:
        df = spark.read.format("rss").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "title" in df.columns
        assert "link" in df.columns
    except Exception as exc:
        msg = str(exc).lower()
        if "timeout" in msg or "connection" in msg:
            pytest.skip("RSS feed unavailable")
        raise

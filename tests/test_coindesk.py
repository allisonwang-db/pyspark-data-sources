"""Tests for CoinDeskDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import CoinDeskDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_coindesk_datasource_registration(spark):
    spark.dataSource.register(CoinDeskDataSource)
    assert CoinDeskDataSource.name() == "coindesk"


def test_coindesk_read(spark):
    spark.dataSource.register(CoinDeskDataSource)
    try:
        df = spark.read.format("coindesk").load()
        rows = df.collect()
        if len(rows) == 0:
            pytest.skip("CoinDesk API returned no data (may be unavailable)")
        assert len(rows) == 1
        assert "rate" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

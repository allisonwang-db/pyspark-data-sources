"""Tests for AdviceDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import AdviceDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_advice_datasource_registration(spark):
    spark.dataSource.register(AdviceDataSource)
    assert AdviceDataSource.name() == "advice"


def test_advice_read(spark):
    spark.dataSource.register(AdviceDataSource)
    try:
        df = spark.read.format("advice").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "advice" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

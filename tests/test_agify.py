"""Tests for AgifyDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import AgifyDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_agify_datasource_registration(spark):
    spark.dataSource.register(AgifyDataSource)
    assert AgifyDataSource.name() == "agify"


def test_agify_read(spark):
    spark.dataSource.register(AgifyDataSource)
    try:
        df = spark.read.format("agify").option("path", "Michael").load()
        rows = df.collect()
        assert len(rows) == 1
        assert "name" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

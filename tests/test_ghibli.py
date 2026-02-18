"""Tests for GhibliDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import GhibliDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_ghibli_datasource_registration(spark):
    spark.dataSource.register(GhibliDataSource)
    assert GhibliDataSource.name() == "ghibli"


def test_ghibli_read(spark):
    spark.dataSource.register(GhibliDataSource)
    try:
        df = spark.read.format("ghibli").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "title" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

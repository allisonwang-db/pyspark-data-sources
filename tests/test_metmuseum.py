"""Tests for MetMuseumDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import MetMuseumDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_metmuseum_datasource_registration(spark):
    spark.dataSource.register(MetMuseumDataSource)
    assert MetMuseumDataSource.name() == "metmuseum"


def test_metmuseum_read(spark):
    spark.dataSource.register(MetMuseumDataSource)
    try:
        df = spark.read.format("metmuseum").option("limit", "10").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "title" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

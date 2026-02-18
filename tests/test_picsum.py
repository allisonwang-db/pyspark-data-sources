"""Tests for PicsumDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import PicsumDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_picsum_datasource_registration(spark):
    spark.dataSource.register(PicsumDataSource)
    assert PicsumDataSource.name() == "picsum"


def test_picsum_read(spark):
    spark.dataSource.register(PicsumDataSource)
    try:
        df = spark.read.format("picsum").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "author" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

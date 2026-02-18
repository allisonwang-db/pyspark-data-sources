"""Tests for FakeStoreDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import FakeStoreDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_fakestore_datasource_registration(spark):
    spark.dataSource.register(FakeStoreDataSource)
    assert FakeStoreDataSource.name() == "fakestore"


def test_fakestore_read(spark):
    spark.dataSource.register(FakeStoreDataSource)
    try:
        df = spark.read.format("fakestore").load()
        rows = df.collect()
        if len(rows) == 0:
            pytest.skip("Fake Store API returned no data (may be unavailable)")
        assert "title" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

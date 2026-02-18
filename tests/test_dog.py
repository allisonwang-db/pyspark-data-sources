"""Tests for DogDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import DogDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_dog_datasource_registration(spark):
    spark.dataSource.register(DogDataSource)
    assert DogDataSource.name() == "dog"


def test_dog_read(spark):
    spark.dataSource.register(DogDataSource)
    try:
        df = spark.read.format("dog").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "breed" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

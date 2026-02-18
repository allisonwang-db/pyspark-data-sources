"""Tests for TriviaDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import TriviaDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_trivia_datasource_registration(spark):
    spark.dataSource.register(TriviaDataSource)
    assert TriviaDataSource.name() == "trivia"


def test_trivia_read(spark):
    spark.dataSource.register(TriviaDataSource)
    try:
        df = spark.read.format("trivia").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "question" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

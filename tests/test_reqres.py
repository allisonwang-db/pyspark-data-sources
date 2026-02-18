"""Tests for ReqresDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import ReqresDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_reqres_datasource_registration(spark):
    spark.dataSource.register(ReqresDataSource)
    assert ReqresDataSource.name() == "reqres"


def test_reqres_read(spark):
    spark.dataSource.register(ReqresDataSource)
    try:
        df = spark.read.format("reqres").load()
        rows = df.collect()
        if len(rows) == 0:
            pytest.skip("ReqRes API returned no data (may be unavailable)")
        assert "email" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

"""Tests for AzureGraphDataSource."""

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import AzureGraphDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_azuregraph_datasource_registration(spark):
    spark.dataSource.register(AzureGraphDataSource)
    assert AzureGraphDataSource.name() == "azuregraph"


def test_azuregraph_requires_token(spark):
    spark.dataSource.register(AzureGraphDataSource)
    df = spark.read.format("azuregraph").load()
    with pytest.raises(Exception, match="token|access_token"):
        df.collect()


def test_azuregraph_read_with_invalid_token(spark):
    """With invalid token, request fails - we return empty iterator on RequestException."""
    spark.dataSource.register(AzureGraphDataSource)
    df = spark.read.format("azuregraph").option("token", "invalid").option("path", "users").load()
    rows = df.collect()
    assert isinstance(rows, list)

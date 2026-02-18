"""Tests for CocktailDataSource."""

import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import CocktailDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_cocktail_registration(spark):
    spark.dataSource.register(CocktailDataSource)
    assert CocktailDataSource.name() == "cocktail"

def test_cocktail_read(spark):
    spark.dataSource.register(CocktailDataSource)
    try:
        df = spark.read.format("cocktail").load("margarita")
        assert df.count() > 0
    except Exception as e:
        if "timeout" in str(e).lower():
            pytest.skip("Network unavailable")
        raise

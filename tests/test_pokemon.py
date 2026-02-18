"""Tests for PokemonDataSource."""

import pytest

from pyspark.sql import SparkSession

from pyspark_datasources import PokemonDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_pokemon_datasource_registration(spark):
    spark.dataSource.register(PokemonDataSource)
    assert PokemonDataSource.name() == "pokemon"


def test_pokemon_read(spark):
    spark.dataSource.register(PokemonDataSource)
    try:
        df = spark.read.format("pokemon").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "name" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise

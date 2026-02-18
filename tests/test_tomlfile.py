"""Tests for TomlFileDataSource."""

import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import TomlFileDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_tomlfile_registration(spark):
    spark.dataSource.register(TomlFileDataSource)
    assert TomlFileDataSource.name() == "tomlfile"

def test_tomlfile_read(spark):
    try:
        import tomllib
    except ImportError:
        try:
            import tomli
        except ImportError:
            pytest.skip("tomli not installed")
    spark.dataSource.register(TomlFileDataSource)
    with tempfile.NamedTemporaryFile(suffix=".toml", delete=False) as f:
        f.write(b'[section]\nkey = "value"')
        f.flush()
        path = f.name
    try:
        df = spark.read.format("tomlfile").load(path)
        assert df.count() > 0
    finally:
        import os
        os.unlink(path)

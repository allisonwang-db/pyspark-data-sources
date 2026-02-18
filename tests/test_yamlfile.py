"""Tests for YamlFileDataSource."""

import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import YamlFileDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_yamlfile_registration(spark):
    spark.dataSource.register(YamlFileDataSource)
    assert YamlFileDataSource.name() == "yamlfile"

def test_yamlfile_read(spark):
    try:
        import yaml
    except ImportError:
        pytest.skip("pyyaml not installed")
    spark.dataSource.register(YamlFileDataSource)
    with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
        f.write(b"name: test\nvalue: 42")
        f.flush()
        path = f.name
    try:
        df = spark.read.format("yamlfile").load(path)
        assert df.count() > 0
    finally:
        import os
        os.unlink(path)

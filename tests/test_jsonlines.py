"""Tests for JsonLinesDataSource."""

import tempfile
import pytest
from pyspark.sql import SparkSession
from pyspark_datasources import JsonLinesDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_jsonlines_registration(spark):
    spark.dataSource.register(JsonLinesDataSource)
    assert JsonLinesDataSource.name() == "jsonlines"

def test_jsonlines_read(spark):
    spark.dataSource.register(JsonLinesDataSource)
    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
        f.write(b'{"a":1,"b":2}\n{"x":"y"}\n')
        f.flush()
        path = f.name
    try:
        df = spark.read.format("jsonlines").load(path)
        assert df.count() == 2
    finally:
        import os
        os.unlink(path)

def test_jsonlines_requires_path(spark):
    spark.dataSource.register(JsonLinesDataSource)
    with pytest.raises((ValueError, Exception), match="path"):
        spark.read.format("jsonlines").load().collect()

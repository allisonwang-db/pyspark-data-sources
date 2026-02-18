"""Tests for DockerHubDataSource."""

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import DockerHubDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_dockerhub_datasource_registration(spark):
    """Test that DockerHubDataSource can be registered."""
    spark.dataSource.register(DockerHubDataSource)
    assert DockerHubDataSource.name() == "dockerhub"


def test_dockerhub_read(spark):
    """Test reading Docker image tags (integration - uses real API)."""
    spark.dataSource.register(DockerHubDataSource)
    try:
        df = spark.read.format("dockerhub").option("page_size", "5").load("library/redis")
        rows = df.collect()
        assert len(rows) > 0
        assert "name" in df.columns
        assert "digest" in df.columns
    except Exception as exc:
        msg = str(exc).lower()
        if "timeout" in msg or "connection" in msg:
            pytest.skip("Docker Hub API unavailable")
        raise

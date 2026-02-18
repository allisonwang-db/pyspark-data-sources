"""Tests for RedditDataSource."""

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import RedditDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_reddit_datasource_registration(spark):
    """Test that RedditDataSource can be registered."""
    spark.dataSource.register(RedditDataSource)
    assert RedditDataSource.name() == "reddit"


def test_reddit_read(spark):
    """Test reading subreddit posts (integration - uses real API)."""
    spark.dataSource.register(RedditDataSource)
    try:
        df = spark.read.format("reddit").option("limit", "5").load("python")
        rows = df.collect()
        assert len(rows) > 0
        assert "title" in df.columns
        assert "author" in df.columns
    except Exception as exc:
        msg = str(exc).lower()
        if "429" in msg or "timeout" in msg or "connection" in msg or "blocked" in msg:
            pytest.skip("Reddit API unavailable or rate limited")
        raise

import pytest

from pyspark.sql import SparkSession
from pyspark_datasources.github import GithubPullRequestSource


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


def test_github_datasource(spark):
    spark.dataSource.register(GithubPullRequestSource)
    df = spark.read.format("github").load("apache/spark")
    prs = df.collect()
    assert len(prs) > 0

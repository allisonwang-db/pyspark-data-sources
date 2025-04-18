import pytest

from pyspark.sql import SparkSession
from pyspark_datasources import *


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


def test_github_datasource(spark):
    spark.dataSource.register(GithubDataSource)
    df = spark.read.format("github").load("apache/spark")
    prs = df.collect()
    assert len(prs) > 0


def test_fake_datasource(spark):
    spark.dataSource.register(FakeDataSource)
    df = spark.read.format("fake").load()
    df.show()
    assert df.count() == 3
    assert len(df.columns) == 4


def test_kaggle_datasource(spark):
    spark.dataSource.register(KaggleDataSource)
    df = spark.read.format("kaggle").options(handle="yasserh/titanic-dataset").load("Titanic-Dataset.csv")
    df.show()
    assert df.count() == 891
    assert len(df.columns) == 12

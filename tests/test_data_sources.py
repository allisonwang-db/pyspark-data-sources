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


def test_google_sheets(spark):
    spark.dataSource.register(GoogleSheetsDataSource)
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=846122797#gid=846122797"
    df = spark.read.format("googlesheets").options(url=url).load()
    df.show()
    assert df.count() == 2
    assert len(df.columns) == 2

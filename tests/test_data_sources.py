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


def test_fake_datasource_stream(spark):
    spark.dataSource.register(FakeDataSource)
    (
        spark.readStream.format("fake")
        .load()
        .writeStream.format("memory")
        .queryName("result")
        .trigger(once=True)
        .start()
        .awaitTermination()
    )
    spark.sql("SELECT * FROM result").show()
    assert spark.sql("SELECT * FROM result").count() == 3


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


def test_opensky_datasource_stream(spark):
    spark.dataSource.register(OpenSkyDataSource)
    (
        spark.readStream.format("opensky")
        .option("region", "EUROPE")
        .load()
        .writeStream.format("memory")
        .queryName("opensky_result") 
        .trigger(once=True)
        .start()
        .awaitTermination()
    )
    result = spark.sql("SELECT * FROM opensky_result")
    result.show()
    assert len(result.columns) == 18  # Check schema has expected number of fields
    assert result.count() > 0  # Verify we got some data

def test_jsonplaceholder_posts():
     spark.dataSource.register(JSONPlaceholderDataSource)
     posts_df = spark.read.format("jsonplaceholder").option("endpoint", "posts").load()
     assert posts_df.count() > 0 # Ensure we have some posts


def test_jsonplaceholder_referential_integrity():
    spark.dataSource.register(JSONPlaceholderDataSource)
    users_df = spark.read.format("jsonplaceholder").option("endpoint", "users").load()
    posts_df = spark.read.format("jsonplaceholder").option("endpoint", "posts").load()
    posts_with_authors = posts_df.join(users_df, posts_df.userId == users_df.id)
    assert posts_with_authors.count() > 0  # Ensure join is valid and we have posts with authors

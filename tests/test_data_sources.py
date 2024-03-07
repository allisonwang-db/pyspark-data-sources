import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import FakeDataSource, GithubDataSource, RestapiDataSource


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

def test_restapi_datasource_with_params(spark):
    spark.dataSource.register(RestapiDataSource)
    df = (
        spark.read.format("restapi")
        .option("method", "GET")
        .option("params.page", "2")
        .load("reqres.in/api/users")
    )
    assert df.count() > 0
    assert len(df.columns) == 9

def test_restapi_datasource_basic_auth(spark):
    spark.dataSource.register(RestapiDataSource)
    df = (
        spark.read.format("restapi")
        .option("method", "GET")
        .option("auth.type", "BASIC")
        .option("auth.client-id", "test")
        .option("auth.client-secret", "test")
        .load("httpbin.org/basic-auth/test/test")
    )
    assert df.count() > 0
    assert len(df.columns) == 9

def test_restapi_datasource_bearer(spark):
    spark.dataSource.register(RestapiDataSource)
    df = (
        spark.read.format("restapi")
        .option("method", "GET")
        .option("headers.Authorization", "Bearer token123")
        .load("httpbin.org/bearer")
    )
    assert df.count() > 0
    assert len(df.columns) == 9

def test_restapi_datasource_post(spark):
    spark.dataSource.register(RestapiDataSource)
    df = (
        spark.read.format("restapi")
        .option("method", "POST")
        .option("json_data.key_a", "value_a")
        .option("json_data.key_b.nested_key", "value_b")
        .load("httpbin.org/post")
    )
    assert df.count() > 0
    assert len(df.columns) == 9
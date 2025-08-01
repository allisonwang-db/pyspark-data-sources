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


def test_salesforce_datasource_registration(spark):
    """Test that Salesforce DataSource can be registered and validates required options."""
    spark.dataSource.register(SalesforceDataSource)
    
    # Test that the datasource is registered with correct name
    assert SalesforceDataSource.name() == "salesforce-sink"
    
    # Test that the data source is streaming-only (no batch writer)
    from pyspark.sql.functions import lit
    
    try:
        # Try to use batch write - should fail since we only support streaming
        df = spark.range(1).select(
            lit("Test Company").alias("Name"),
            lit("Technology").alias("Industry"),
            lit(50000.0).alias("AnnualRevenue")
        )
        
        df.write.format("salesforce-sink").mode("append").save()
        assert False, "Should have raised error - Salesforce DataSource only supports streaming"
    except Exception as e:
        # This is expected - Salesforce DataSource only supports streaming writes
        error_msg = str(e).lower()
        # The error can be about unsupported mode or missing writer
        assert "unsupported" in error_msg or "writer" in error_msg or "not implemented" in error_msg

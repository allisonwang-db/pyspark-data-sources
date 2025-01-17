import pytest
from pyspark.errors.exceptions.captured import AnalysisException, PythonException

from pyspark_datasources import GoogleSheetsDataSource

from .test_data_sources import spark


def test_url(spark):
    spark.dataSource.register(GoogleSheetsDataSource)
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=846122797#gid=846122797"
    df = spark.read.format("googlesheets").options(url=url).load()
    df.show()
    assert df.count() == 2
    assert len(df.columns) == 2
    assert df.schema.simpleString() == "struct<num:string,name:string>"


def test_spreadsheet_id(spark):
    spark.dataSource.register(GoogleSheetsDataSource)
    df = (
        spark.read.format("googlesheets")
        .options(spreadsheet_id="10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0")
        .load()
    )
    df.show()
    assert df.count() == 2
    assert len(df.columns) == 2


def test_missing_options(spark):
    spark.dataSource.register(GoogleSheetsDataSource)
    with pytest.raises(AnalysisException) as excinfo:
        spark.read.format("googlesheets").load()
    assert "ValueError" in str(excinfo.value)


def test_mutual_exclusive_options(spark):
    spark.dataSource.register(GoogleSheetsDataSource)
    with pytest.raises(AnalysisException) as excinfo:
        spark.read.format("googlesheets").options(
            url="a",
            spreadsheet_id="b",
        ).load()
    assert "ValueError" in str(excinfo.value)


def test_custom_schema(spark):
    spark.dataSource.register(GoogleSheetsDataSource)
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=846122797#gid=846122797"
    df = (
        spark.read.format("googlesheets")
        .options(url=url)
        .schema("a double, b string")
        .load()
    )
    df.show()
    assert df.count() == 2
    assert len(df.columns) == 2
    assert df.schema.simpleString() == "struct<a:double,b:string>"


def test_custom_schema_mismatch_count(spark):
    spark.dataSource.register(GoogleSheetsDataSource)
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=846122797#gid=846122797"
    df = spark.read.format("googlesheets").options(url=url).schema("a double").load()
    with pytest.raises(PythonException) as excinfo:
        df.show()
    assert "CSV parse error" in str(excinfo.value)


def test_custom_schema_mismatch_type(spark):
    spark.dataSource.register(GoogleSheetsDataSource)
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=846122797#gid=846122797"
    df = (
        spark.read.format("googlesheets")
        .options(url=url)
        .schema("a double, b double")
        .load()
    )
    with pytest.raises(PythonException) as excinfo:
        df.show()
    assert "CSV conversion error" in str(excinfo.value)

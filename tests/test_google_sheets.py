import pytest
from pyspark.errors.exceptions.captured import AnalysisException, PythonException
from pyspark.sql import SparkSession

from pyspark_datasources import GoogleSheetsDataSource


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.getOrCreate()
    spark.dataSource.register(GoogleSheetsDataSource)
    yield spark


def test_url(spark):
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=846122797#gid=846122797"
    df = spark.read.format("googlesheets").options(url=url).load()
    df.show()
    assert df.count() == 2
    assert len(df.columns) == 2
    assert df.schema.simpleString() == "struct<num:string,name:string>"


def test_spreadsheet_id(spark):
    df = spark.read.format("googlesheets").load("10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0")
    df.show()
    assert df.count() == 2
    assert len(df.columns) == 2


def test_missing_options(spark):
    with pytest.raises(AnalysisException) as excinfo:
        spark.read.format("googlesheets").load()
    assert "ValueError" in str(excinfo.value)


def test_mutual_exclusive_options(spark):
    with pytest.raises(AnalysisException) as excinfo:
        spark.read.format("googlesheets").options(
            url="a",
            spreadsheet_id="b",
        ).load()
    assert "ValueError" in str(excinfo.value)


def test_custom_schema(spark):
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=846122797#gid=846122797"
    df = spark.read.format("googlesheets").options(url=url).schema("a double, b string").load()
    df.show()
    assert df.count() == 2
    assert len(df.columns) == 2
    assert df.schema.simpleString() == "struct<a:double,b:string>"


def test_custom_schema_mismatch_count(spark):
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=846122797#gid=846122797"
    df = spark.read.format("googlesheets").options(url=url).schema("a double").load()
    with pytest.raises(PythonException) as excinfo:
        df.show()
    assert "CSV parse error" in str(excinfo.value)


def test_unnamed_column(spark):
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=1579451727#gid=1579451727"
    df = spark.read.format("googlesheets").options(url=url).load()
    df.show()
    assert df.count() == 1
    assert df.columns == ["Unnamed: 0", "1", "Unnamed: 2"]


def test_duplicate_column(spark):
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=1875209731#gid=1875209731"
    df = spark.read.format("googlesheets").options(url=url).load()
    df.show()
    assert df.count() == 1
    assert df.columns == ["a", "a.1"]


def test_no_header_row(spark):
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=1579451727#gid=1579451727"
    df = (
        spark.read.format("googlesheets")
        .schema("a int, b int, c int")
        .options(url=url, has_header="false")
        .load()
    )
    df.show()
    assert df.count() == 2
    assert len(df.columns) == 3


def test_empty(spark):
    url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=2123944555#gid=2123944555"
    with pytest.raises(AnalysisException) as excinfo:
        spark.read.format("googlesheets").options(url=url).load()
    assert "EmptyDataError" in str(excinfo.value)

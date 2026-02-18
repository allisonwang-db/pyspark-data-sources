"""Tests for ExcelDataSource."""

import tempfile

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import ExcelDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_excel_datasource_registration(spark):
    """Test that ExcelDataSource can be registered."""
    spark.dataSource.register(ExcelDataSource)
    assert ExcelDataSource.name() == "excel"


def test_excel_read(spark):
    """Test reading Excel file."""
    try:
        import openpyxl
    except ImportError:
        pytest.skip("openpyxl not installed")

    spark.dataSource.register(ExcelDataSource)

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["A", "B", "C"])
        ws.append([1, 2, 3])
        ws.append([4, 5, 6])
        wb.save(f.name)
        f.flush()
        path = f.name

    try:
        df = spark.read.format("excel").load(path)
        rows = df.collect()
        assert len(rows) >= 2
        assert rows[0]["col_1"] in ("A", "1")
    finally:
        import os

        os.unlink(path)


def test_excel_requires_path(spark):
    """Test that path is required."""
    spark.dataSource.register(ExcelDataSource)
    with pytest.raises((ValueError, Exception), match="path"):
        spark.read.format("excel").load().collect()

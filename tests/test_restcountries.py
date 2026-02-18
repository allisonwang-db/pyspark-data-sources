"""Tests for RestCountriesDataSource."""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from pyspark_datasources import RestCountriesDataSource


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_restcountries_datasource_registration(spark):
    """Test that RestCountriesDataSource can be registered."""
    spark.dataSource.register(RestCountriesDataSource)
    assert RestCountriesDataSource.name() == "restcountries"


def test_restcountries_read_all(spark):
    """Test reading all countries (integration test - uses real API)."""
    spark.dataSource.register(RestCountriesDataSource)
    try:
        df = spark.read.format("restcountries").load()
        rows = df.collect()
        assert len(rows) > 0
        assert "name_common" in df.columns
        assert "population" in df.columns
    except Exception as exc:
        if "timeout" in str(exc).lower() or "connection" in str(exc).lower():
            pytest.skip("Network unavailable")
        raise


def test_restcountries_read_by_name(spark):
    """Test reading country by name with mocked API."""
    spark.dataSource.register(RestCountriesDataSource)

    mock_response = MagicMock()
    mock_response.json.return_value = [
        {
            "name": {"common": "United States", "official": "United States of America"},
            "capital": ["Washington, D.C."],
            "region": "Americas",
            "subregion": "North America",
            "population": 329484123,
            "area": 9833520.0,
            "languages": {"eng": "English"},
            "currencies": {"USD": {"name": "United States dollar", "symbol": "$"}},
        },
    ]
    mock_response.raise_for_status = MagicMock()

    with patch("pyspark_datasources.restcountries.requests.get", return_value=mock_response):
        df = spark.read.format("restcountries").load("usa")
        rows = df.collect()

    assert len(rows) == 1
    assert rows[0]["name_common"] == "United States"
    assert rows[0]["capital"] == "Washington, D.C."

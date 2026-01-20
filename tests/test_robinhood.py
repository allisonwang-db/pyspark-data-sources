import os
import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession, Row
from pyspark.errors.exceptions.captured import AnalysisException

from pyspark_datasources import RobinhoodDataSource


@pytest.fixture
def spark():
    """Create SparkSession for testing."""
    spark = SparkSession.builder.getOrCreate()
    spark.dataSource.register(RobinhoodDataSource)
    yield spark


def test_robinhood_datasource_registration(spark):
    """Test that RobinhoodDataSource can be registered."""
    # Test registration
    assert RobinhoodDataSource.name() == "robinhood"

    # Test schema
    expected_schema = (
        "symbol string, price double, bid_price double, ask_price double, updated_at string"
    )
    datasource = RobinhoodDataSource({})
    assert datasource.schema() == expected_schema


def test_robinhood_missing_credentials(spark):
    """Test that missing API credentials raises an error."""
    with pytest.raises(AnalysisException) as excinfo:
        df = spark.read.format("robinhood").load("BTC-USD")
        df.collect()  # Trigger execution

    assert "ValueError" in str(excinfo.value) and (
        "api_key" in str(excinfo.value) or "private_key" in str(excinfo.value)
    )


def test_robinhood_missing_symbols(spark):
    """Test that missing symbols raises an error."""
    with pytest.raises(AnalysisException) as excinfo:
        df = (
            spark.read.format("robinhood")
            .option("api_key", "test-key")
            .option("private_key", "FAPmPMsQqDFOFiRvpUMJ6BC5eFOh/tPx7qcTYGKc8nE=")
            .load("")
        )
        df.collect()  # Trigger execution

    assert "ValueError" in str(excinfo.value) and "crypto pairs" in str(excinfo.value)


def test_robinhood_invalid_private_key_format(spark):
    """Test that invalid private key format raises proper error."""
    with pytest.raises(AnalysisException) as excinfo:
        df = (
            spark.read.format("robinhood")
            .option("api_key", "test-key")
            .option("private_key", "invalid-key-format")
            .load("BTC-USD")
        )
        df.collect()  # Trigger execution

    assert "Invalid private key format" in str(excinfo.value)


def test_robinhood_btc_data(spark):
    """Test BTC-USD data retrieval with registered API key - REQUIRES API CREDENTIALS."""
    # Get credentials from environment variables
    api_key = os.environ.get("ROBINHOOD_API_KEY")
    private_key = os.environ.get("ROBINHOOD_PRIVATE_KEY")

    if not api_key or not private_key:
        pytest.skip(
            "ROBINHOOD_API_KEY and ROBINHOOD_PRIVATE_KEY environment variables required for real API tests"
        )

    # Test loading BTC-USD data
    df = (
        spark.read.format("robinhood")
        .option("api_key", api_key)
        .option("private_key", private_key)
        .load("BTC-USD")
    )

    rows = df.collect()
    print(f"Retrieved {len(rows)} rows")

    # CRITICAL: Test MUST fail if no data is returned
    assert len(rows) > 0, "TEST FAILED: No data returned! Expected at least 1 BTC-USD record."

    for i, row in enumerate(rows):
        print(f"Row {i + 1}: {row}")

        # Validate data structure
        assert row.symbol == "BTC-USD", f"Expected BTC-USD, got {row.symbol}"
        assert isinstance(
            row.price, (int, float)
        ), f"Price should be numeric, got {type(row.price)}"
        assert row.price > 0, f"Price should be > 0, got {row.price}"
        assert isinstance(
            row.bid_price, (int, float)
        ), f"Bid price should be numeric, got {type(row.bid_price)}"
        assert isinstance(
            row.ask_price, (int, float)
        ), f"Ask price should be numeric, got {type(row.ask_price)}"
        assert isinstance(
            row.updated_at, str
        ), f"Updated timestamp should be string, got {type(row.updated_at)}"


def test_robinhood_multiple_crypto_pairs(spark):
    """Test multi-crypto data retrieval with registered API key - REQUIRES API CREDENTIALS."""
    # Get credentials from environment variables
    api_key = os.environ.get("ROBINHOOD_API_KEY")
    private_key = os.environ.get("ROBINHOOD_PRIVATE_KEY")

    if not api_key or not private_key:
        pytest.skip(
            "ROBINHOOD_API_KEY and ROBINHOOD_PRIVATE_KEY environment variables required for real API tests"
        )

    # Test loading multiple crypto pairs
    df = (
        spark.read.format("robinhood")
        .option("api_key", api_key)
        .option("private_key", private_key)
        .load("BTC-USD,ETH-USD,DOGE-USD")
    )

    rows = df.collect()
    print(f"Retrieved {len(rows)} rows")

    # CRITICAL: Test MUST fail if no data is returned
    assert len(rows) > 0, "TEST FAILED: No data returned! Expected at least 1 crypto record."

    # CRITICAL: Should get data for all 3 requested pairs
    assert len(rows) >= 3, f"TEST FAILED: Expected 3 crypto pairs, got {len(rows)} records."

    symbols_found = set()

    for i, row in enumerate(rows):
        symbols_found.add(row.symbol)
        print(f"Row {i + 1}: {row}")

        # Validate each record
        assert isinstance(row.symbol, str), f"Symbol should be string, got {type(row.symbol)}"
        assert isinstance(
            row.price, (int, float)
        ), f"Price should be numeric, got {type(row.price)}"
        assert row.price > 0, f"Price should be > 0, got {row.price}"
        assert isinstance(
            row.bid_price, (int, float)
        ), f"Bid price should be numeric, got {type(row.bid_price)}"
        assert isinstance(
            row.ask_price, (int, float)
        ), f"Ask price should be numeric, got {type(row.ask_price)}"
        assert isinstance(
            row.updated_at, str
        ), f"Updated timestamp should be string, got {type(row.updated_at)}"

    # Test passes only if we have real data for the requested pairs
    assert (
        len(symbols_found) >= 3
    ), f"Expected at least 3 different symbols, got {len(symbols_found)}: {symbols_found}"

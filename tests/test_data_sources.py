import pytest
import tempfile
import os
from unittest import mock

import pyarrow as pa
import requests
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import MaxRetryError, ReadTimeoutError

from pyspark.sql import SparkSession
from pyspark_datasources import *
from pyspark_datasources.opensky import (
    ConfigOpenSkyError,
    EgressBlockedError,
    OpenSkyClient,
    RateLimitError,
    Region,
    TransientOpenSkyError,
    parse_response,
)


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


def test_github_datasource(spark):
    spark.dataSource.register(GithubDataSource)
    df = spark.read.format("github").load("apache/spark")
    try:
        prs = df.collect()
    except Exception as exc:  # noqa: BLE001 - surface Spark worker errors
        message = str(exc).lower()
        if "rate limit" in message or "403" in message:
            pytest.skip("GitHub API rate limit exceeded")
        raise
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
    df = (
        spark.read.format("kaggle")
        .options(handle="yasserh/titanic-dataset")
        .load("Titanic-Dataset.csv")
    )
    df.show()
    assert df.count() == 891
    assert len(df.columns) == 12


@pytest.mark.skipif(
    not os.environ.get("OPENSKY_LIVE_TEST"),
    reason="hits the live OpenSky API; set OPENSKY_LIVE_TEST=1 to run",
)
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


# --- Offline OpenSky tests (no network) -------------------------------------
# These cover the OpenSkyClient error taxonomy directly, without Spark workers
# or the live API, so they run in CI on every Python version. Anonymous client
# => _ensure_token() is a no-op (no token request); the mock session replaces
# the only network call in fetch_states().


def _anon_client_with_session(mock_session):
    """An anonymous OpenSkyClient whose lazy session is the given mock."""
    client = OpenSkyClient(Region.EUROPE.value, client_id=None, client_secret=None)
    client._session = mock_session  # property returns it since it's not None
    return client


def _read_timeout_connection_error():
    """The exact shape requests raises when urllib3 read retries are exhausted:
    ConnectionError(MaxRetryError(reason=ReadTimeoutError))."""
    pool = HTTPSConnectionPool("opensky-network.org")
    url = "https://opensky-network.org/api/states/all"
    reason = ReadTimeoutError(pool, url, "read timed out")
    return requests.exceptions.ConnectionError(MaxRetryError(pool, url, reason=reason))


def test_fetch_states_read_timeout_is_transient():
    # Regression test for P1 #1: an exhausted read retry arrives as a
    # ConnectionError wrapping a ReadTimeoutError. It must be TRANSIENT (stream
    # self-heals), not EgressBlockedError (which would kill the stream).
    session = mock.Mock()
    session.get.side_effect = _read_timeout_connection_error()
    client = _anon_client_with_session(session)
    # TransientOpenSkyError is disjoint from EgressBlockedError (a
    # ConfigOpenSkyError), so this also proves it is NOT a fatal egress block.
    with pytest.raises(TransientOpenSkyError):
        client.fetch_states()


def test_fetch_states_connect_timeout_is_egress():
    # A dropped TCP connect (no SYN-ACK) is the egress/IP-block signature: loud.
    session = mock.Mock()
    session.get.side_effect = requests.exceptions.ConnectTimeout("connect timed out")
    client = _anon_client_with_session(session)
    with pytest.raises(EgressBlockedError):
        client.fetch_states()


def test_fetch_states_429_is_rate_limit():
    session = mock.Mock()
    session.get.return_value = mock.Mock(status_code=429)
    client = _anon_client_with_session(session)
    with pytest.raises(RateLimitError):
        client.fetch_states()


def test_fetch_states_401_is_config_error():
    # 401 with no token to refresh (anonymous) is a deterministic config fault.
    session = mock.Mock()
    session.get.return_value = mock.Mock(status_code=401)
    client = _anon_client_with_session(session)
    with pytest.raises(ConfigOpenSkyError):
        client.fetch_states()


def test_parse_response_field_mapping():
    # Guards the (schema-silent) field mapping: geo_altitude<-state[13],
    # baro_altitude<-state[7], category<-state[17] (extended=1 layout).
    state = [
        "abc123", "TEST    ", "Testland", 1700000000, 1700000001,
        8.5, 50.0, 1000.0, False, 200.0, 180.0, 0.0, None, 1050.0,
        "1000", False, 0, 2,
    ]  # indices 0..17; [7]=baro=1000, [13]=geo=1050, [17]=category=2
    rows = parse_response({"time": 1700000002, "states": [state]})
    assert len(rows) == 1
    row = rows[0]
    # parse_state tuple positions: 8=geo_altitude, 14=baro_altitude, 17=category
    assert row[8] == 1050.0  # geo_altitude
    assert row[14] == 1000.0  # baro_altitude
    assert row[8] > row[14]  # geo > baro, as OpenSky docs specify
    assert row[17] == 2  # category from state[17]


def test_salesforce_datasource_registration(spark):
    """Test that Salesforce DataSource can be registered and validates required options."""
    spark.dataSource.register(SalesforceDataSource)

    # Test that the datasource is registered with correct name
    assert SalesforceDataSource.name() == "pyspark.datasource.salesforce"

    # Test that the data source is streaming-only (no batch writer)
    from pyspark.sql.functions import lit

    try:
        # Try to use batch write - should fail since we only support streaming
        df = spark.range(1).select(
            lit("Test Company").alias("Name"),
            lit("Technology").alias("Industry"),
            lit(50000.0).alias("AnnualRevenue"),
        )

        df.write.format("pyspark.datasource.salesforce").mode("append").save()
        assert False, "Should have raised error - Salesforce DataSource only supports streaming"
    except Exception as e:
        # This is expected - Salesforce DataSource only supports streaming writes
        error_msg = str(e).lower()
        # The error can be about unsupported mode or missing writer
        assert "unsupported" in error_msg or "writer" in error_msg or "not implemented" in error_msg


def test_arrow_datasource_single_file(spark):
    """Test reading a single Arrow file."""
    spark.dataSource.register(ArrowDataSource)

    # Create test data
    test_data = pa.table(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]}
    )

    # Write to temporary Arrow file
    with tempfile.NamedTemporaryFile(suffix=".arrow", delete=False) as tmp_file:
        tmp_path = tmp_file.name

    try:
        with pa.ipc.new_file(tmp_path, test_data.schema) as writer:
            writer.write_table(test_data)

        # Read using Arrow data source
        df = spark.read.format("arrow").load(tmp_path)

        # Verify results
        assert df.count() == 3
        assert len(df.columns) == 3
        assert set(df.columns) == {"id", "name", "age"}

        # Verify data content
        rows = df.collect()
        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"

    finally:
        # Clean up
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def test_arrow_datasource_multiple_files(spark):
    """Test reading multiple Arrow files from a directory."""
    spark.dataSource.register(ArrowDataSource)

    # Create test data for multiple files
    test_data1 = pa.table(
        {"id": [1, 2], "name": ["Alice", "Bob"], "department": ["Engineering", "Sales"]}
    )

    test_data2 = pa.table(
        {"id": [3, 4], "name": ["Charlie", "Diana"], "department": ["Marketing", "HR"]}
    )

    # Create temporary directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Write multiple Arrow files
        file1_path = os.path.join(tmp_dir, "data1.arrow")
        file2_path = os.path.join(tmp_dir, "data2.arrow")

        with pa.ipc.new_file(file1_path, test_data1.schema) as writer:
            writer.write_table(test_data1)

        with pa.ipc.new_file(file2_path, test_data2.schema) as writer:
            writer.write_table(test_data2)

        # Read using Arrow data source from directory
        df = spark.read.format("arrow").load(tmp_dir)

        # Verify results
        assert df.count() == 4  # 2 rows from each file
        assert len(df.columns) == 3
        assert set(df.columns) == {"id", "name", "department"}

        # Verify partitioning (should have 2 partitions, one per file)
        assert df.rdd.getNumPartitions() == 2

        # Verify all data is present
        rows = df.collect()
        names = {row["name"] for row in rows}
        assert names == {"Alice", "Bob", "Charlie", "Diana"}


def test_jsonplaceholder_posts(spark):
    spark.dataSource.register(JSONPlaceholderDataSource)
    posts_df = spark.read.format("jsonplaceholder").option("endpoint", "posts").load()
    assert posts_df.count() > 0  # Ensure we have some posts


def test_jsonplaceholder_referential_integrity(spark):
    spark.dataSource.register(JSONPlaceholderDataSource)
    users_df = spark.read.format("jsonplaceholder").option("endpoint", "users").load()
    assert users_df.count() > 0  # Ensure we have some users
    posts_df = spark.read.format("jsonplaceholder").option("endpoint", "posts").load()
    posts_with_authors = posts_df.join(users_df, posts_df.userId == users_df.id)
    assert posts_with_authors.count() > 0  # Ensure join is valid and we have posts with authors

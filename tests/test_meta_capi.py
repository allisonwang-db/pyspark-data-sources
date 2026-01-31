import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark_datasources.meta_capi import (
    MetaCapiStreamWriter,
    MetaCapiBatchWriter,
    MetaCapiCommitMessage,
)


@pytest.fixture
def writers():
    schema = StructType(
        [
            StructField("event_name", StringType()),
            StructField("email", StringType()),
            StructField("value", DoubleType()),
            StructField("event_time", LongType()),
        ]
    )
    options = {"access_token": "fake_token", "pixel_id": "12345", "batch_size": "2"}
    stream_writer = MetaCapiStreamWriter(schema, options)
    batch_writer = MetaCapiBatchWriter(schema, options)
    return {"stream": stream_writer, "batch": batch_writer}


def test_transform_row_to_event(writers):
    stream_writer = writers["stream"]

    # Mock a Row object
    row = MagicMock()
    row.event_name = "Purchase"
    row.event_time = 1600000000
    row.email = "test@example.com"
    row.value = 50.0
    # Missing fields return None
    row.phone = None
    row.user_data = None
    row.custom_data = None

    # Test with stream writer (uses common logic)
    event = stream_writer._transform_row_to_event(row)

    assert event["event_name"] == "Purchase"
    assert event["event_time"] == 1600000000
    # Check hashing
    expected_hash = "973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b"  # sha256("test@example.com")
    assert event["user_data"]["em"] == expected_hash
    assert event["custom_data"]["value"] == 50.0


@patch("requests.post")
def test_send_batch(mock_post, writers):
    batch_writer = writers["batch"]

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"events_received": 2}
    mock_post.return_value = mock_response

    events = [{"event_name": "Test1"}, {"event_name": "Test2"}]
    stats = {"succeeded": 0, "failed": 0}

    # Test with batch writer (uses common logic)
    batch_writer._send_batch(events, stats)

    assert stats["succeeded"] == 2
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args

    assert "https://graph.facebook.com/v19.0/12345/events" in args[0]
    assert kwargs["json"]["access_token"] == "fake_token"
    assert len(kwargs["json"]["data"]) == 2


def test_batch_writer_commit(writers):
    batch_writer = writers["batch"]
    # Ensure commit doesn't raise error
    messages = [
        MetaCapiCommitMessage(batch_id=1, events_processed=10, events_succeeded=9, events_failed=1),
        MetaCapiCommitMessage(batch_id=2, events_processed=5, events_succeeded=5, events_failed=0),
    ]
    try:
        batch_writer.commit(messages)
    except Exception as e:
        pytest.fail(f"commit() raised Exception: {e}")


def test_transform_with_structs(writers):
    stream_writer = writers["stream"]
    # Test explicit structs for user_data
    row = MagicMock()
    row.event_name = "PageView"
    row.event_time = 1234567890

    # user_data struct mock
    user_data_mock = MagicMock()
    user_data_mock.asDict.return_value = {"em": "hashed_already"}
    row.user_data = user_data_mock

    # custom_data struct mock
    custom_data_mock = MagicMock()
    custom_data_mock.asDict.return_value = {"status": "gold"}
    row.custom_data = custom_data_mock

    event = stream_writer._transform_row_to_event(row)

    assert event["user_data"]["em"] == "hashed_already"
    assert event["custom_data"]["status"] == "gold"

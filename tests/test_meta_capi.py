import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark_datasources.meta_capi import MetaCapiStreamWriter, MetaCapiBatchWriter, MetaCapiCommitMessage

class TestMetaCapiWriters(unittest.TestCase):
    def setUp(self):
        self.schema = StructType([
            StructField("event_name", StringType()),
            StructField("email", StringType()),
            StructField("value", DoubleType()),
            StructField("event_time", LongType())
        ])
        self.options = {
            "access_token": "fake_token",
            "pixel_id": "12345",
            "batch_size": "2"
        }
        self.stream_writer = MetaCapiStreamWriter(self.schema, self.options)
        self.batch_writer = MetaCapiBatchWriter(self.schema, self.options)

    def test_transform_row_to_event(self):
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
        event = self.stream_writer._transform_row_to_event(row)
        
        self.assertEqual(event["event_name"], "Purchase")
        self.assertEqual(event["event_time"], 1600000000)
        # Check hashing
        expected_hash = "f660ab912ec121d1b1e928a0bb4bc61b15f5ad44d5efdc4e1c92a25e99b8e44a" # sha256("test@example.com")
        self.assertEqual(event["user_data"]["em"], expected_hash)
        self.assertEqual(event["custom_data"]["value"], 50.0)

    @patch("requests.post")
    def test_send_batch(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"events_received": 2}
        mock_post.return_value = mock_response

        events = [{"event_name": "Test1"}, {"event_name": "Test2"}]
        stats = {"succeeded": 0, "failed": 0}
        
        # Test with batch writer (uses common logic)
        self.batch_writer._send_batch(events, stats)
        
        self.assertEqual(stats["succeeded"], 2)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        
        self.assertIn("https://graph.facebook.com/v19.0/12345/events", args[0])
        self.assertEqual(kwargs["json"]["access_token"], "fake_token")
        self.assertEqual(len(kwargs["json"]["data"]), 2)

    def test_batch_writer_commit(self):
        # Ensure commit doesn't raise error
        messages = [
            MetaCapiCommitMessage(batch_id=1, events_processed=10, events_succeeded=9, events_failed=1),
            MetaCapiCommitMessage(batch_id=2, events_processed=5, events_succeeded=5, events_failed=0)
        ]
        try:
            self.batch_writer.commit(messages)
        except Exception as e:
            self.fail(f"commit() raised Exception: {e}")

    def test_transform_with_structs(self):
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
        
        event = self.stream_writer._transform_row_to_event(row)
        
        self.assertEqual(event["user_data"]["em"], "hashed_already")
        self.assertEqual(event["custom_data"]["status"], "gold")

if __name__ == "__main__":
    unittest.main()

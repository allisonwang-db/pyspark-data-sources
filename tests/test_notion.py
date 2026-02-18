import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, Row
from pyspark_datasources.notion import NotionDataSource, NotionDataSourceReader, NotionDataSourceWriter

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_notion_datasource_registration(spark):
    spark.dataSource.register(NotionDataSource)
    assert NotionDataSource.name() == "notion"

def test_notion_reader_logic():
    options = {"token": "fake_token", "database_id": "fake_db_id"}
    reader = NotionDataSourceReader(options)
    
    mock_response = {
        "results": [
            {
                "id": "page-id-1",
                "created_time": "2023-01-01T00:00:00.000Z",
                "last_edited_time": "2023-01-02T00:00:00.000Z",
                "url": "https://notion.so/page-1",
                "properties": {
                    "Name": {
                        "type": "title",
                        "title": [{"plain_text": "Test Page 1"}]
                    },
                    "Status": {
                        "type": "select",
                        "select": {"name": "Done"}
                    }
                }
            }
        ],
        "has_more": False,
        "next_cursor": None
    }

    with patch("notion_client.Client") as MockClient:
        mock_instance = MockClient.return_value
        mock_instance.databases.query.return_value = mock_response

        # Call read directly (partition is ignored in our implementation)
        iterator = reader.read(None)
        rows = list(iterator)
        
        assert len(rows) == 1
        row = rows[0]
        assert row.id == "page-id-1"
        assert row.title == "Test Page 1"
        assert "Status" in row.properties

def test_notion_writer_logic():
    options = {"token": "fake_token", "database_id": "fake_db_id"}
    writer = NotionDataSourceWriter(options)
    
    data = [
        Row(properties='{"Name": {"title": [{"text": {"content": "New Page"}}]}}')
    ]
    
    with patch("notion_client.Client") as MockClient:
        mock_instance = MockClient.return_value
        mock_instance.pages.create.return_value = {"id": "new-page-id"}

        # Call write directly
        writer.write(iter(data))
        
        mock_instance.pages.create.assert_called_once()
        call_args = mock_instance.pages.create.call_args
        assert call_args.kwargs["parent"] == {"database_id": "fake_db_id"}
        assert "Name" in call_args.kwargs["properties"]

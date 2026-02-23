from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row, SparkSession

from pyspark_datasources.qdrant import QdrantDataSource, QdrantReader, QdrantWriter


@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def test_qdrant_datasource_registration(spark):
    spark.dataSource.register(QdrantDataSource)
    assert QdrantDataSource.name() == "qdrant"


def test_qdrant_reader_requires_url_and_collection():
    with pytest.raises(ValueError, match="option 'url' is required"):
        QdrantReader({"collection": "products"})

    with pytest.raises(ValueError, match="option 'collection' is required"):
        QdrantReader({"url": "http://localhost:6333"})


def test_qdrant_reader_invalid_filter_json():
    with pytest.raises(ValueError, match="must be valid JSON"):
        QdrantReader(
            {
                "url": "http://localhost:6333",
                "collection": "products",
                "filter": "{invalid-json}",
            }
        )


def test_qdrant_reader_scroll_pagination():
    reader = QdrantReader(
        {
            "url": "http://localhost:6333",
            "collection": "products",
            "limit": "2",
            "with_payload": "true",
            "with_vector": "true",
        }
    )

    response_1 = MagicMock()
    response_1.json.return_value = {
        "result": {
            "points": [
                {
                    "id": 1,
                    "vector": [0.1, 0.2],
                    "payload": {"category": "books"},
                    "order_value": 10,
                }
            ],
            "next_page_offset": 1,
        }
    }
    response_1.raise_for_status.return_value = None

    response_2 = MagicMock()
    response_2.json.return_value = {
        "result": {
            "points": [
                {
                    "id": "2",
                    "vector": {"default": [0.3, 0.4]},
                    "payload": {"category": "tech"},
                    "shard_key": "tenant-1",
                    "order_value": 20,
                }
            ],
            "next_page_offset": None,
        }
    }
    response_2.raise_for_status.return_value = None

    with patch("pyspark_datasources.qdrant.requests.post", side_effect=[response_1, response_2]) as post:
        rows = list(reader.read(None))

        assert len(rows) == 2
        assert rows[0].id == "1"
        assert rows[0].vector == "[0.1, 0.2]"
        assert rows[0].payload == '{"category": "books"}'
        assert rows[0].shard_key is None
        assert rows[0].order_value == 10

        assert rows[1].id == "2"
        assert rows[1].vector == '{"default": [0.3, 0.4]}'
        assert rows[1].payload == '{"category": "tech"}'
        assert rows[1].shard_key == "tenant-1"
        assert rows[1].order_value == 20

        assert post.call_count == 2

        first_call_body = post.call_args_list[0].kwargs["json"]
        second_call_body = post.call_args_list[1].kwargs["json"]

        assert first_call_body["limit"] == 2
        assert "offset" not in first_call_body
        assert second_call_body["offset"] == 1


def test_qdrant_writer_requires_url_and_collection():
    with pytest.raises(ValueError, match="option 'url' is required"):
        QdrantWriter({"collection": "products"})

    with pytest.raises(ValueError, match="option 'collection' is required"):
        QdrantWriter({"url": "http://localhost:6333"})


def test_qdrant_writer_batches_and_upserts_points():
    writer = QdrantWriter(
        {
            "url": "http://localhost:6333",
            "collection": "products",
            "batch_size": "2",
            "wait": "true",
            "api_key": "secret",
        }
    )

    response_1 = MagicMock()
    response_1.raise_for_status.return_value = None
    response_2 = MagicMock()
    response_2.raise_for_status.return_value = None

    rows = [
        Row(id="1", vector="[0.1, 0.2]", payload='{"category": "books"}', shard_key=None),
        Row(id="2", vector=[0.3, 0.4], payload={"category": "tech"}, shard_key="tenant-1"),
        Row(id="3", vector='{"image": [0.5, 0.6]}', payload=None, shard_key=None),
    ]

    with patch("pyspark_datasources.qdrant.TaskContext.get", return_value=None):
        with patch(
            "pyspark_datasources.qdrant.requests.put", side_effect=[response_1, response_2]
        ) as put:
            msg = writer.write(iter(rows))

    assert msg.records_written == 3
    assert put.call_count == 2

    first_call = put.call_args_list[0]
    second_call = put.call_args_list[1]

    assert first_call.kwargs["params"] == {"wait": "true"}
    assert first_call.kwargs["headers"]["api-key"] == "secret"
    assert first_call.kwargs["json"]["points"] == [
        {"id": "1", "vector": [0.1, 0.2], "payload": {"category": "books"}},
        {
            "id": "2",
            "vector": [0.3, 0.4],
            "payload": {"category": "tech"},
            "shard_key": "tenant-1",
        },
    ]
    assert second_call.kwargs["json"]["points"] == [
        {"id": "3", "vector": {"image": [0.5, 0.6]}}
    ]


def test_qdrant_writer_rejects_invalid_json_vector():
    writer = QdrantWriter(
        {
            "url": "http://localhost:6333",
            "collection": "products",
        }
    )

    rows = [Row(id="1", vector="{bad-json}", payload=None, shard_key=None)]

    with pytest.raises(ValueError, match="field 'vector' must be valid JSON"):
        writer.write(iter(rows))

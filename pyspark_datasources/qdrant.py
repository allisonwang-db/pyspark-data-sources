import json
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import requests
from pyspark import TaskContext
from pyspark.sql import Row
from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import StructType


class QdrantDataSource(DataSource):
    """
    A PySpark data source for reading points from Qdrant collections.

    Name: `qdrant`

    Schema:
    `id string, vector string, payload string, shard_key string, order_value long`

    Required options:
    - url: Base Qdrant URL, e.g. http://localhost:6333
    - collection: Qdrant collection name

    Optional options:
    - api_key: Qdrant API key
    - limit: Page size for each scroll request (default: 100)
    - with_payload: Include payload data (default: true)
    - with_vector: Include vector data (default: true)
    - filter: JSON filter object for Qdrant scroll
    - batch_size: Number of points per write request (default: 100)
    - wait: Wait until write is applied (default: true)

    Examples
    --------
    Register the data source:

    >>> from pyspark_datasources import QdrantDataSource
    >>> spark.dataSource.register(QdrantDataSource)

    Read all points from a collection:

    >>> df = spark.read.format("qdrant") \
    ...     .option("url", "http://localhost:6333") \
    ...     .option("collection", "products") \
    ...     .load()
    >>> df.show()

    Read with a filter:

    >>> df = spark.read.format("qdrant") \
    ...     .option("url", "http://localhost:6333") \
    ...     .option("collection", "products") \
    ...     .option("filter", '{"must": [{"key": "category", "match": {"value": "books"}}]}') \
    ...     .load()

    Write points to a collection:

    >>> points_df.write.format("qdrant") \
    ...     .option("url", "http://localhost:6333") \
    ...     .option("collection", "products") \
    ...     .save()
    """

    @classmethod
    def name(cls) -> str:
        return "qdrant"

    def schema(self) -> str:
        return "id string, vector string, payload string, shard_key string, order_value long"

    def reader(self, schema: StructType) -> "QdrantReader":
        return QdrantReader(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> "QdrantWriter":
        return QdrantWriter(self.options)


class QdrantOptions:
    def __init__(self, options: Dict[str, str]):
        self.options = options
        self.url = (options.get("url") or "").rstrip("/")
        self.collection = options.get("collection")
        self.api_key = options.get("api_key")

        if not self.url:
            raise ValueError("Qdrant option 'url' is required.")
        if not self.collection:
            raise ValueError("Qdrant option 'collection' is required.")


class QdrantReader(DataSourceReader):
    def __init__(self, options: Dict[str, str]):
        self.options = options
        qdrant_options = QdrantOptions(options)
        self.url = qdrant_options.url
        self.collection = qdrant_options.collection
        self.api_key = qdrant_options.api_key
        self.limit = int(options.get("limit", "100"))
        self.with_payload = self._as_bool(options.get("with_payload", "true"))
        self.with_vector = self._as_bool(options.get("with_vector", "true"))
        self.filter = self._parse_filter(options.get("filter"))
        if self.limit <= 0:
            raise ValueError("Qdrant option 'limit' must be a positive integer.")

    @staticmethod
    def _as_bool(value: str) -> bool:
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    @staticmethod
    def _parse_filter(value: Optional[str]) -> Optional[Dict[str, Any]]:
        if not value:
            return None
        try:
            parsed = json.loads(value)
            if not isinstance(parsed, dict):
                raise ValueError("Qdrant option 'filter' must be a JSON object.")
            return parsed
        except json.JSONDecodeError as exc:
            raise ValueError("Qdrant option 'filter' must be valid JSON.") from exc

    def partitions(self) -> List[InputPartition]:
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[Row]:
        endpoint = f"{self.url}/collections/{self.collection}/points/scroll"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["api-key"] = self.api_key

        offset = None

        try:
            while True:
                body: Dict[str, Any] = {
                    "limit": self.limit,
                    "with_payload": self.with_payload,
                    "with_vector": self.with_vector,
                }
                if self.filter:
                    body["filter"] = self.filter
                if offset is not None:
                    body["offset"] = offset

                response = requests.post(endpoint, json=body, headers=headers, timeout=30)
                response.raise_for_status()

                payload = response.json().get("result", {})
                points = payload.get("points", [])

                for point in points:
                    yield Row(
                        id=str(point.get("id")) if point.get("id") is not None else None,
                        vector=self._to_json(point.get("vector")),
                        payload=self._to_json(point.get("payload")),
                        shard_key=(
                            str(point.get("shard_key"))
                            if point.get("shard_key") is not None
                            else None
                        ),
                        order_value=point.get("order_value"),
                    )

                offset = payload.get("next_page_offset")
                if offset is None:
                    break
        except requests.RequestException as exc:
            print(f"Failed to read from Qdrant collection '{self.collection}': {exc}")
            return iter([])
        except (TypeError, ValueError) as exc:
            print(f"Failed to parse Qdrant response for '{self.collection}': {exc}")
            return iter([])

    @staticmethod
    def _to_json(value: Any) -> Optional[str]:
        if value is None:
            return None
        return json.dumps(value)


@dataclass
class QdrantCommitMessage(WriterCommitMessage):
    records_written: int
    batch_id: int


class QdrantWriter(DataSourceWriter):
    def __init__(self, options: Dict[str, str]):
        self.options = options
        qdrant_options = QdrantOptions(options)
        self.url = qdrant_options.url
        self.collection = qdrant_options.collection
        self.api_key = qdrant_options.api_key
        self.batch_size = int(options.get("batch_size", "100"))
        self.wait = QdrantReader._as_bool(options.get("wait", "true"))

        if self.batch_size <= 0:
            raise ValueError("Qdrant option 'batch_size' must be a positive integer.")

    def write(self, iterator: Iterator[Row]) -> QdrantCommitMessage:
        endpoint = f"{self.url}/collections/{self.collection}/points"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["api-key"] = self.api_key

        context = TaskContext.get()
        batch_id = context.taskAttemptId() if context else 0
        records_written = 0
        buffer: List[Dict[str, Any]] = []

        for row in iterator:
            buffer.append(self._to_point(row))
            if len(buffer) >= self.batch_size:
                self._flush(endpoint, headers, buffer)
                records_written += len(buffer)
                buffer = []

        if buffer:
            self._flush(endpoint, headers, buffer)
            records_written += len(buffer)

        return QdrantCommitMessage(records_written=records_written, batch_id=batch_id)

    def _flush(self, endpoint: str, headers: Dict[str, str], points: List[Dict[str, Any]]) -> None:
        response = requests.put(
            endpoint,
            params={"wait": str(self.wait).lower()},
            json={"points": points},
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()

    def _to_point(self, row: Row) -> Dict[str, Any]:
        values = row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)

        point: Dict[str, Any] = {}
        point_id = values.get("id")
        if point_id is not None:
            point["id"] = point_id

        vector = self._from_json_or_value(values.get("vector"), "vector")
        if vector is not None:
            point["vector"] = vector

        payload = self._from_json_or_value(values.get("payload"), "payload")
        if payload is not None:
            point["payload"] = payload

        shard_key = values.get("shard_key")
        if shard_key is not None:
            point["shard_key"] = shard_key

        return point

    @staticmethod
    def _from_json_or_value(value: Any, field_name: str) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Qdrant field '{field_name}' must be valid JSON when provided as string."
                ) from exc
        return value

    def commit(self, messages: List[QdrantCommitMessage]) -> None:
        pass

    def abort(self, messages: List[QdrantCommitMessage]) -> None:
        pass

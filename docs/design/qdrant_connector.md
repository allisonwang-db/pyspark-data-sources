# Qdrant Data Source Design Document

## Overview

This document describes the design of a PySpark Data Source for Qdrant that supports batch reading and batch writing of vector points and metadata.

The connector enables Spark jobs to:
- read points from a Qdrant collection (via scroll pagination), and
- write/upsert points into a Qdrant collection.

## Motivation

Qdrant is commonly used for vector search and retrieval workloads. Adding first-class Spark integration simplifies ingestion pipelines for embeddings, metadata enrichment, and analytics workflows.

## Design

### Dependencies

The connector uses:
- `requests` for HTTP calls to Qdrant REST API.
- `pyspark` Python Data Source API interfaces.

No additional optional package is required beyond project defaults.

### Data Source Name

The data source is registered as `qdrant`.

### Options

| Option | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `url` | Base URL of the Qdrant service (for example, `http://localhost:6333`). | Yes | - |
| `collection` | Qdrant collection name. | Yes | - |
| `api_key` | API key for authenticated deployments. | No | - |
| `limit` | Page size per scroll request (read only). Must be positive integer. | No | `100` |
| `with_payload` | Include payload in read results (read only). | No | `true` |
| `with_vector` | Include vector in read results (read only). | No | `true` |
| `filter` | JSON object as string for Qdrant read filtering (read only). | No | - |
| `batch_size` | Number of points per write request (write only). Must be positive integer. | No | `100` |
| `wait` | Wait for write operation completion on Qdrant side (write only). | No | `true` |

## Schema

The connector uses a fixed schema for both read and write:

```sql
id string,
vector string,
payload string,
shard_key string,
order_value long
```

Notes:
- `vector` and `payload` are represented as JSON strings on read.
- On write, `vector` and `payload` can be JSON strings or native Spark-compatible values.

## Read Path (Batch)

Implemented by `QdrantReader`.

1. Validate required options (`url`, `collection`) and parse optional settings.
2. Call Qdrant Scroll API endpoint:
   - `POST /collections/{collection}/points/scroll`
3. Build request body with `limit`, `with_payload`, `with_vector`, optional `filter`, and optional `offset`.
4. Iterate pages using `next_page_offset` until pagination is exhausted.
5. Convert each point into Spark Row:
   - `id` as string
   - `vector`, `payload` serialized to JSON strings
   - `shard_key` string (if present)
   - `order_value` long (if present)

### Example (Read)

```python
df = (
    spark.read.format("qdrant")
    .option("url", "http://localhost:6333")
    .option("collection", "products")
    .option("limit", "200")
    .load()
)
```

## Write Path (Batch)

Implemented by `QdrantWriter`.

1. Validate required options (`url`, `collection`) and parse write settings.
2. Iterate input rows and convert each row into Qdrant point payload:
   - pass through `id`
   - parse `vector` and `payload` from JSON strings when necessary
   - include optional `shard_key`
3. Buffer points up to `batch_size`.
4. Upsert batched points using:
   - `PUT /collections/{collection}/points?wait={wait}`
5. Return commit metadata with count of records written.

### Example (Write)

```python
(
    points_df.write.format("qdrant")
    .option("url", "http://localhost:6333")
    .option("collection", "products")
    .option("batch_size", "500")
    .option("wait", "true")
    .mode("append")
    .save()
)
```

## Error Handling

- Missing `url` or `collection` raises `ValueError`.
- Non-positive `limit` or `batch_size` raises `ValueError`.
- Invalid JSON in `filter` raises `ValueError`.
- Invalid JSON in write-time `vector`/`payload` string fields raises `ValueError`.
- HTTP/network errors from Qdrant are surfaced via request exceptions.

## Testing

Unit tests cover:
- datasource registration,
- required option validation,
- read pagination behavior,
- write batching and upsert payload shape,
- invalid JSON handling for read and write paths.

## Future Improvements

- Add optional dynamic schema mode (separate typed columns for payload fields).
- Support partitioned parallel reads across shards when exposed by query options.
- Add advanced write controls (ordering mode, retry/backoff policy, id strategy helpers).

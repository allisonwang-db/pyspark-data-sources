# Qdrant Data Source Example

Read vectors and payload metadata from a Qdrant collection using the Scroll API.

## Setup Credentials

If your Qdrant deployment requires authentication, set an API key:

```python
QDRANT_API_KEY = "your_qdrant_api_key"
```

For local development without auth, you can skip `api_key`.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import QdrantDataSource

spark = SparkSession.builder.appName("qdrant-example").getOrCreate()
spark.dataSource.register(QdrantDataSource)
```

### Step 2: Read Points from a Collection

```python
df = (
    spark.read.format("qdrant")
    .option("url", "http://localhost:6333")
    .option("collection", "products")
    .option("limit", "200")
    .load()
)

df.select("id", "vector", "payload").show(truncate=False)
```

### Step 3: Optional - Read with Payload Filter

```python
import json

qdrant_filter = {
    "must": [
        {"key": "category", "match": {"value": "books"}}
    ]
}

filtered_df = (
    spark.read.format("qdrant")
    .option("url", "http://localhost:6333")
    .option("collection", "products")
    .option("api_key", QDRANT_API_KEY)
    .option("filter", json.dumps(qdrant_filter))
    .load()
)

filtered_df.show(truncate=False)
```

### Example Output

```
+----+----------------------------+------------------------------------------+
|id  |vector                      |payload                                   |
+----+----------------------------+------------------------------------------+
|1001|[0.12,0.45,0.78,0.03]       |{"category":"books","price":19.99}  |
|1002|[0.18,0.33,0.62,0.11]       |{"category":"books","price":24.50}  |
+----+----------------------------+------------------------------------------+
```

## End-to-End Pipeline: Batch Write

### Step 1: Build a DataFrame of Qdrant Points

```python
from pyspark.sql import Row

points = [
    Row(
        id=1001,
        vector=[0.12, 0.45, 0.78, 0.03],
        payload={"category": "books", "price": 19.99},
        shard_key=None,
    ),
    Row(
        id=1002,
        vector=[0.18, 0.33, 0.62, 0.11],
        payload={"category": "books", "price": 24.50},
        shard_key="tenant-1",
    ),
]

points_df = spark.createDataFrame(points)
```

### Step 2: Upsert Points into Qdrant

```python
(
    points_df.write.format("qdrant")
    .option("url", "http://localhost:6333")
    .option("collection", "products")
    .option("api_key", QDRANT_API_KEY)
    .option("batch_size", "500")
    .option("wait", "true")
    .mode("append")
    .save()
)
```

`vector` and `payload` columns can be either:
- valid JSON strings (as shown above), or
- native Spark values (array/map/struct) that serialize to JSON-compatible objects.

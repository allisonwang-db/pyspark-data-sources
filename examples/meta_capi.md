# Meta Conversions API Data Source Example

Write event data to Meta (Facebook) for ad optimization. Supports batch and streaming write.

## Setup Credentials

```python
META_ACCESS_TOKEN = "your-system-user-access-token"
META_PIXEL_ID = "your-pixel-id"
```

Create credentials in Meta Business Suite: Events Manager → Data Sources → Add new → Conversions API.
Use a System User token with `ads_management` and `business_management` permissions.

## End-to-End Pipeline: Batch Write

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, unix_timestamp
from pyspark_datasources import MetaCapiDataSource

spark = SparkSession.builder.appName("meta-capi-batch").getOrCreate()
spark.dataSource.register(MetaCapiDataSource)
```

### Step 2: Create Sample Event Data

```python
from pyspark.sql import Row

events = [
    Row(event_name="Purchase", event_time=1705312800, email="test@example.com", action_source="website", value=99.99, currency="USD", test_event_code="TEST12345"),
    Row(event_name="Lead", event_time=1705312860, email="user2@example.com", action_source="website", value=0.0, currency="USD", test_event_code="TEST12345"),
]
events_df = spark.createDataFrame(events)
```

### Step 3: Write to Meta CAPI (Batch)

```python
(
    events_df.write.format("meta_capi")
    .option("access_token", META_ACCESS_TOKEN)
    .option("pixel_id", META_PIXEL_ID)
    .option("test_event_code", "TEST12345")
    .save()  # Path not used for Meta CAPI
)
```

## End-to-End Pipeline: Streaming Write

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark_datasources import MetaCapiDataSource

spark = SparkSession.builder.appName("meta-capi-stream").getOrCreate()
spark.dataSource.register(MetaCapiDataSource)
```

### Step 2: Create Streaming Source and Transform to CAPI Format

```python
streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

events_df = streaming_df.select(
    lit("Purchase").alias("event_name"),
    col("timestamp").alias("event_time"),
    lit("test@example.com").alias("email"),
    lit("website").alias("action_source"),
    (col("value") * 10.0).alias("value"),
    lit("USD").alias("currency"),
    lit("TEST12345").alias("test_event_code"),
)
```

### Step 3: Write Stream to Meta CAPI

```python
query = (
    events_df.writeStream.format("meta_capi")
    .option("access_token", META_ACCESS_TOKEN)
    .option("pixel_id", META_PIXEL_ID)
    .option("test_event_code", "TEST12345")
    .option("checkpointLocation", "/tmp/meta_capi_checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)
# query.awaitTermination()  # Run until stopped
```

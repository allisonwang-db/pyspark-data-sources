# Python Data Source API Reference

Complete reference for the Python Data Source API introduced in Apache Spark 4.0.

## Core Abstract Base Classes

### DataSource

The primary abstract base class for custom data sources supporting read/write operations.

```python
class DataSource:
    def __init__(self, options: Dict[str, str])
    def name() -> str
    def schema() -> StructType
    def reader(schema: StructType) -> DataSourceReader
    def writer(schema: StructType, overwrite: bool) -> DataSourceWriter
    def streamReader(schema: StructType) -> DataSourceStreamReader
    def streamWriter(schema: StructType, overwrite: bool) -> DataSourceStreamWriter
    def simpleStreamReader(schema: StructType) -> SimpleDataSourceStreamReader
```

#### Methods

| Method | Required | Description | Default |
|--------|----------|-------------|---------|
| `__init__(options)` | No | Initialize with user options | Base class provides default |
| `name()` | No | Return format name for registration | Class name |
| `schema()` | Yes | Define the data source schema | No default |
| `reader(schema)` | If batch read | Create batch reader | Not implemented |
| `writer(schema, overwrite)` | If batch write | Create batch writer | Not implemented |
| `streamReader(schema)` | If streaming* | Create streaming reader | Not implemented |
| `streamWriter(schema, overwrite)` | If stream write | Create streaming writer | Not implemented |
| `simpleStreamReader(schema)` | If streaming* | Create simple streaming reader | Not implemented |

*For streaming read, implement either `streamReader` or `simpleStreamReader`, not both.

### DataSourceReader

Abstract base class for reading data from sources in batch mode.

```python
class DataSourceReader:
    def read(partition) -> Iterator
    def partitions() -> List[InputPartition]
```

#### Methods

| Method | Required | Description | Default |
|--------|----------|-------------|---------|
| `read(partition)` | Yes | Read data from partition | No default |
| `partitions()` | No | Return input partitions for parallel reading | Single partition |

#### Return Types for read()

The `read()` method can return:
- **Tuples**: Matching the schema field order
- **Row objects**: `from pyspark.sql import Row`
- **pyarrow.RecordBatch**: For better performance

### DataSourceWriter

Abstract base class for writing data to external sources in batch mode.

```python
class DataSourceWriter:
    def write(iterator) -> WriterCommitMessage
    def commit(messages: List[WriterCommitMessage]) -> WriteResult
    def abort(messages: List[WriterCommitMessage]) -> None
```

#### Methods

| Method | Required | Description |
|--------|----------|-------------|
| `write(iterator)` | Yes | Write data from iterator, return commit message |
| `commit(messages)` | Yes | Commit successful writes |
| `abort(messages)` | No | Handle write failures and cleanup |

### DataSourceStreamReader

Abstract base class for streaming data sources with full offset management and partition planning.

```python
class DataSourceStreamReader:
    def initialOffset() -> dict
    def latestOffset() -> dict
    def partitions(start: dict, end: dict) -> List[InputPartition]
    def read(partition) -> Iterator
    def commit(end: dict) -> None
    def stop() -> None
```

#### Methods

| Method | Required | Description |
|--------|----------|-------------|
| `initialOffset()` | Yes | Return starting offset |
| `latestOffset()` | Yes | Return latest available offset |
| `partitions(start, end)` | Yes | Get partitions for offset range |
| `read(partition)` | Yes | Read data from partition |
| `commit(end)` | No | Mark offsets as processed |
| `stop()` | No | Clean up resources |

### SimpleDataSourceStreamReader

Simplified streaming reader interface without partition planning. Choose this over `DataSourceStreamReader` when your data source doesn't naturally partition.

```python
class SimpleDataSourceStreamReader:
    def initialOffset() -> dict
    def read(start: dict) -> Tuple[Iterator, dict]
    def readBetweenOffsets(start: dict, end: dict) -> Iterator
    def commit(end: dict) -> None
```

#### Methods

| Method | Required | Description |
|--------|----------|-------------|
| `initialOffset()` | Yes | Return starting offset |
| `read(start)` | Yes | Read from start offset, return (iterator, next_offset) |
| `readBetweenOffsets(start, end)` | Recommended | Deterministic replay between offsets |
| `commit(end)` | No | Mark offsets as processed |

### DataSourceStreamWriter

Abstract base class for writing data to external sinks in streaming queries.

```python
class DataSourceStreamWriter:
    def write(iterator) -> WriterCommitMessage
    def commit(messages: List[WriterCommitMessage], batchId: int) -> None
    def abort(messages: List[WriterCommitMessage], batchId: int) -> None
```

#### Methods

| Method | Required | Description |
|--------|----------|-------------|
| `write(iterator)` | Yes | Write data for a partition |
| `commit(messages, batchId)` | Yes | Commit successful microbatch writes |
| `abort(messages, batchId)` | No | Handle write failures for a microbatch |

## Helper Classes

### InputPartition

Represents a partition of input data.

```python
class InputPartition:
    def __init__(self, value: Any)
```

The `value` can be any serializable Python object that identifies the partition (int, dict, tuple, etc.).

### WriterCommitMessage

Message returned from successful write operations.

```python
class WriterCommitMessage:
    def __init__(self, value: Any)
```

The `value` typically contains metadata about the write (e.g., file path, row count).

### WriteResult

Final result after committing all writes.

```python
class WriteResult:
    def __init__(self, **kwargs)
```

Can contain any metadata about the completed write operation.

## Implementation Requirements

### 1. Serialization

All classes must be pickle-serializable. This means:

```python
# ❌ BAD - Connection objects can't be pickled
class BadReader(DataSourceReader):
    def __init__(self, options):
        import psycopg2
        self.connection = psycopg2.connect(options['url'])

# ✅ GOOD - Store configuration, create connections in read()
class GoodReader(DataSourceReader):
    def __init__(self, options):
        self.connection_url = options['url']

    def read(self, partition):
        import psycopg2
        conn = psycopg2.connect(self.connection_url)
        try:
            # Use connection
            yield from fetch_data(conn)
        finally:
            conn.close()
```

### 2. Schema Definition

Use PySpark's type system:

```python
from pyspark.sql.types import *

def schema(self):
    return StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("tags", ArrayType(StringType()), nullable=True),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
    ])
```

### 3. Data Types Support

Supported Spark SQL data types:

| Python Type | Spark Type | Notes |
|-------------|------------|-------|
| `int` | `IntegerType`, `LongType` | Use LongType for large integers |
| `float` | `FloatType`, `DoubleType` | DoubleType is default for decimals |
| `str` | `StringType` | Unicode strings |
| `bool` | `BooleanType` | True/False |
| `datetime.date` | `DateType` | Date without time |
| `datetime.datetime` | `TimestampType` | Date with time |
| `bytes` | `BinaryType` | Raw bytes |
| `list` | `ArrayType(elementType)` | Homogeneous arrays |
| `dict` | `MapType(keyType, valueType)` | Key-value pairs |
| `tuple/Row` | `StructType([fields])` | Nested structures |

### 4. Error Handling

Implement proper exception handling:

```python
def read(self, partition):
    try:
        data = fetch_external_data()
        yield from data
    except NetworkError as e:
        if self.options.get("failOnError", "true") == "true":
            raise DataSourceError(f"Failed to fetch data: {e}")
        else:
            # Log and continue
            logger.warning(f"Skipping partition due to error: {e}")
            return  # Empty iterator
```

### 5. Resource Management

Clean up resources properly:

```python
class ManagedReader(DataSourceStreamReader):
    def stop(self):
        """Called when streaming query stops."""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.close()
        if hasattr(self, 'temp_files'):
            for f in self.temp_files:
                os.remove(f)
```

### 6. File Path Convention

When dealing with file paths:

```python
# ✅ CORRECT - Path in load()
df = spark.read.format("myformat").load("/path/to/data")

# ❌ INCORRECT - Path in option()
df = spark.read.format("myformat").option("path", "/path/to/data").load()
```

### 7. Lazy Initialization

Defer expensive operations:

```python
class LazyReader(DataSourceReader):
    def __init__(self, options):
        # ✅ Just store configuration
        self.api_url = options['url']
        self.api_key = options['apiKey']

    def read(self, partition):
        # ✅ Expensive operations here
        session = create_authenticated_session(self.api_key)
        data = session.get(self.api_url)
        yield from parse_data(data)
```

## Performance Optimization Techniques

### 1. Arrow Integration

Return `pyarrow.RecordBatch` for better serialization:

```python
import pyarrow as pa

def read(self, partition):
    # Read data efficiently
    data = fetch_data_batch()

    # Convert to Arrow
    batch = pa.RecordBatch.from_pandas(data)
    yield batch
```

Benefits:
- Zero-copy data transfer when possible
- Columnar format efficiency
- Better memory usage

### 2. Partitioning Strategy

Effective partitioning for parallelism:

```python
def partitions(self):
    # Example: Partition by date ranges
    dates = get_date_range(self.start_date, self.end_date)
    return [InputPartition({"date": date}) for date in dates]

def read(self, partition):
    date = partition.value["date"]
    # Read only data for this date
    yield from read_data_for_date(date)
```

Guidelines:
- Aim for equal-sized partitions
- Consider data locality
- Balance between too many small partitions and too few large ones

### 3. Batch Processing

Process data in batches:

```python
def read(self, partition):
    batch_size = 1000
    offset = 0

    while True:
        batch = fetch_batch(offset, batch_size)
        if not batch:
            break

        # Process entire batch at once
        processed = process_batch(batch)
        yield from processed

        offset += batch_size
```

### 4. Connection Pooling

Reuse connections within partitions:

```python
def read(self, partition):
    # Create connection once per partition
    conn = create_connection()
    try:
        # Reuse for all records in partition
        for record_id in partition.value["ids"]:
            data = fetch_with_connection(conn, record_id)
            yield data
    finally:
        conn.close()
```

### 5. Caching Strategies

Cache frequently accessed data:

```python
class CachedReader(DataSourceReader):
    _cache = {}  # Class-level cache

    def read(self, partition):
        cache_key = partition.value["key"]

        if cache_key not in self._cache:
            self._cache[cache_key] = fetch_expensive_data(cache_key)

        data = self._cache[cache_key]
        yield from process_data(data)
```

## Usage Examples

### Registration and Basic Usage

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("datasource-example").getOrCreate()

# Register the data source
spark.dataSource.register(MyDataSource)

# Use with format()
df = spark.read.format("myformat").option("key", "value").load()

# Use with streaming
stream = spark.readStream.format("myformat").load()
```

### Schema Handling

```python
# Let data source define schema
df = spark.read.format("myformat").load()

# Override with custom schema
custom_schema = StructType([
    StructField("id", LongType()),
    StructField("value", DoubleType())
])
df = spark.read.format("myformat").schema(custom_schema).load()

# Schema subset - read only specific columns
df = spark.read.format("myformat").load().select("id", "name")
```

### Options Pattern

```python
df = spark.read.format("myformat") \
    .option("url", "https://api.example.com") \
    .option("apiKey", "secret-key") \
    .option("timeout", "30") \
    .option("retries", "3") \
    .option("batchSize", "1000") \
    .load()
```

## Common Pitfalls and Solutions

### Pitfall 1: Non-Serializable Classes

**Problem**: Storing database connections, HTTP clients, or other non-serializable objects as instance variables.

**Solution**: Create these objects in `read()` method.

### Pitfall 2: Ignoring Schema Parameter

**Problem**: Always using `self.schema()` instead of the schema parameter passed to `reader()`.

**Solution**: Respect user-specified schema for column pruning.

### Pitfall 3: Blocking in Constructor

**Problem**: Making API calls or heavy I/O in `__init__`.

**Solution**: Defer to `read()` method for lazy evaluation.

### Pitfall 4: No Partitioning

**Problem**: Not implementing `partitions()`, limiting parallelism.

**Solution**: Implement logical partitioning based on your data source.

### Pitfall 5: Poor Error Messages

**Problem**: Generic exceptions without context.

**Solution**: Provide detailed, actionable error messages.

### Pitfall 6: Resource Leaks

**Problem**: Not cleaning up connections, files, or memory.

**Solution**: Use try/finally blocks and implement `stop()` for streaming.

### Pitfall 7: Inefficient Data Transfer

**Problem**: Yielding individual rows for large datasets.

**Solution**: Use Arrow RecordBatch or batch processing.

## Testing Guidelines

### Unit Testing

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def test_reader(spark):
    spark.dataSource.register(MyDataSource)
    df = spark.read.format("myformat").load()
    assert df.count() > 0
    assert df.schema == expected_schema
```

### Integration Testing

```python
def test_with_real_data(spark, real_api_key):
    spark.dataSource.register(MyDataSource)

    df = spark.read.format("myformat") \
        .option("apiKey", real_api_key) \
        .load()

    # Verify real data
    assert df.count() > 0
    df.write.mode("overwrite").parquet("/tmp/test_output")
```

### Performance Testing

```python
def test_performance(spark, benchmark):
    spark.dataSource.register(MyDataSource)

    with benchmark("read_performance"):
        df = spark.read.format("myformat") \
            .option("numRows", "1000000") \
            .load()
        count = df.count()

    assert count == 1000000
    benchmark.assert_timing(max_seconds=10)
```

## Additional Resources

- [Apache Spark Python Data Source API](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html)
- [Source Code](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py)
- [Example Implementations](https://github.com/allisonwang-db/pyspark-data-sources)
- [Simple Stream Reader Architecture](simple-stream-reader-architecture.md)
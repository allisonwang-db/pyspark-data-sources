# Building Custom Data Sources

This guide teaches you how to create your own custom data sources using the Python Data Source API in Apache Spark 4.0+.

## Table of Contents
- [Basic Implementation Pattern](#basic-implementation-pattern)
- [Advanced Features](#advanced-features)
- [Best Practices](#best-practices)
- [Common Patterns](#common-patterns)
- [Debugging and Testing](#debugging-and-testing)

## Basic Implementation Pattern

Every custom data source starts with two classes: a `DataSource` and a `DataSourceReader`.

### Minimal Example

```python
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class MyCustomDataSource(DataSource):
    """Custom data source implementation."""

    def __init__(self, options):
        """Initialize with user-provided options."""
        self.options = options

    def name(self):
        """Return the short name for this data source."""
        return "mycustom"

    def schema(self):
        """Define the schema for this data source."""
        return StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("value", StringType(), nullable=True)
        ])

    def reader(self, schema):
        """Create a reader for batch data."""
        return MyCustomReader(self.options, schema)

class MyCustomReader(DataSourceReader):
    """Reader implementation for the custom data source."""

    def __init__(self, options, schema):
        self.options = options
        self.schema = schema

    def read(self, partition):
        """
        Read data from the source.
        Yields tuples matching the schema.
        """
        # Example: Read from an API, file, database, etc.
        for i in range(10):
            yield (i, f"name_{i}", f"value_{i}")

# Register and use
spark.dataSource.register(MyCustomDataSource)
df = spark.read.format("mycustom").load()
df.show()
```

## Advanced Features

### 1. Partitioned Reading for Parallelism

Split your data into partitions to leverage Spark's parallel processing capabilities.

```python
from pyspark.sql.datasource import InputPartition

class PartitionedReader(DataSourceReader):
    def __init__(self, options, schema):
        self.options = options
        self.schema = schema
        self.total_records = int(options.get("totalRecords", "1000"))

    def partitions(self):
        """Split data into partitions for parallel processing."""
        num_partitions = int(self.options.get("numPartitions", "4"))
        records_per_partition = self.total_records // num_partitions

        partitions = []
        for i in range(num_partitions):
            start = i * records_per_partition
            end = start + records_per_partition if i < num_partitions - 1 else self.total_records
            partitions.append(InputPartition({"start": start, "end": end}))

        return partitions

    def read(self, partition):
        """Read only this partition's data."""
        partition_info = partition.value
        start = partition_info["start"]
        end = partition_info["end"]

        for i in range(start, end):
            yield (i, f"data_{i}", f"value_{i}")
```

### 2. Streaming Data Source

Implement streaming capabilities with offset management.

```python
from pyspark.sql.datasource import DataSourceStreamReader, InputPartition
import time
import json

class MyStreamReader(DataSourceStreamReader):
    def __init__(self, options, schema):
        self.options = options
        self.schema = schema
        self.rate = int(options.get("rowsPerSecond", "10"))

    def initialOffset(self):
        """Return the initial offset to start reading from."""
        return {"timestamp": 0, "offset": 0}

    def latestOffset(self):
        """Return the latest available offset."""
        current_time = int(time.time())
        # Calculate how many records should be available
        if not hasattr(self, '_start_time'):
            self._start_time = current_time
            self._last_offset = 0

        elapsed = current_time - self._start_time
        latest = elapsed * self.rate
        return {"timestamp": current_time, "offset": latest}

    def partitions(self, start, end):
        """Return partitions between start and end offsets."""
        start_offset = start["offset"]
        end_offset = end["offset"]

        # Create a single partition for simplicity
        # In production, you'd split this into multiple partitions
        return [InputPartition({"start": start_offset, "end": end_offset})]

    def read(self, partition):
        """Read data for the partition."""
        partition_data = partition.value
        start_offset = partition_data["start"]
        end_offset = partition_data["end"]

        # Generate data between offsets
        for offset in range(int(start_offset), int(end_offset)):
            timestamp = self._start_time + (offset // self.rate)
            yield (offset, f"stream_data_{offset}", timestamp)

    def commit(self, end):
        """Commit the offset after successful processing."""
        print(f"Committed offset: {end}")

    def stop(self):
        """Clean up resources when streaming stops."""
        print("Streaming stopped")
```

### 3. Simple Streaming Reader (Without Partitions)

For simpler streaming scenarios where partitioning adds unnecessary complexity.

```python
from pyspark.sql.datasource import SimpleDataSourceStreamReader
import time

class SimpleStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, options, schema):
        self.options = options
        self.schema = schema
        self.batch_size = int(options.get("batchSize", "100"))

    def initialOffset(self):
        """Return the initial offset."""
        return {"offset": 0}

    def read(self, start):
        """Read from start offset and return data with next offset."""
        current_offset = start["offset"]

        # Generate batch of data
        data = []
        for i in range(self.batch_size):
            record_id = current_offset + i
            data.append((record_id, f"data_{record_id}", time.time()))

        # Return data iterator and next offset
        next_offset = {"offset": current_offset + self.batch_size}
        return iter(data), next_offset

    def readBetweenOffsets(self, start, end):
        """Read deterministically between offsets for recovery."""
        start_offset = start["offset"]
        end_offset = end["offset"]

        data = []
        for i in range(start_offset, end_offset):
            data.append((i, f"data_{i}", "replay"))

        return iter(data)

    def commit(self, end):
        """Optional: Mark offset as processed."""
        print(f"Processed up to offset: {end}")
```

### 4. Arrow-Optimized Reader

Use PyArrow for better performance with large datasets.

```python
import pyarrow as pa
import numpy as np

class ArrowOptimizedReader(DataSourceReader):
    def __init__(self, options, schema):
        self.options = options
        self.schema = schema
        self.batch_size = int(options.get("batchSize", "10000"))

    def read(self, partition):
        """Return data as PyArrow RecordBatch for better performance."""
        # Generate data efficiently using NumPy
        ids = np.arange(self.batch_size)
        names = [f"name_{i}" for i in ids]
        values = np.random.random(self.batch_size)

        # Create Arrow RecordBatch
        batch = pa.RecordBatch.from_arrays([
            pa.array(ids),
            pa.array(names),
            pa.array(values)
        ], names=["id", "name", "value"])

        yield batch

    def partitions(self):
        """Create multiple partitions for parallel Arrow processing."""
        num_partitions = int(self.options.get("numPartitions", "4"))
        return [InputPartition(i) for i in range(num_partitions)]
```

### 5. Data Source with Write Support

Implement both reading and writing capabilities.

```python
from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
import json

class MyDataSourceWithWrite(DataSource):
    def writer(self, schema, overwrite):
        """Create a writer for batch data."""
        return MyWriter(self.options, schema, overwrite)

class MyWriter(DataSourceWriter):
    def __init__(self, options, schema, overwrite):
        self.options = options
        self.schema = schema
        self.overwrite = overwrite
        self.path = options.get("path", "/tmp/output")

    def write(self, iterator):
        """Write data from iterator to destination."""
        import tempfile
        import os

        # Write to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False,
                                        suffix='.json', dir=self.path) as f:
            count = 0
            for row in iterator:
                # Convert row to dict and write as JSON
                row_dict = row.asDict() if hasattr(row, 'asDict') else dict(zip(self.schema.names, row))
                json.dump(row_dict, f)
                f.write('\n')
                count += 1

            temp_path = f.name

        # Return commit message with metadata
        return WriterCommitMessage({"file": temp_path, "count": count})

    def commit(self, messages):
        """Commit all successful writes."""
        total_count = 0
        files = []

        for msg in messages:
            total_count += msg.value["count"]
            files.append(msg.value["file"])

        print(f"Successfully wrote {total_count} records to {len(files)} files")
        return {"status": "success", "files": files, "total_count": total_count}

    def abort(self, messages):
        """Clean up on failure."""
        import os
        for msg in messages:
            if msg and "file" in msg.value:
                try:
                    os.remove(msg.value["file"])
                except:
                    pass
        print("Write aborted, temporary files cleaned up")
```

## Best Practices

### 1. Always Validate Options

```python
def __init__(self, options):
    # Validate required options
    if "api_key" not in options:
        raise ValueError("api_key is required for this data source")

    # Parse options with defaults and type checking
    self.api_key = options["api_key"]
    self.batch_size = int(options.get("batchSize", "1000"))
    self.timeout = float(options.get("timeout", "30.0"))

    # Validate option values
    if self.batch_size <= 0:
        raise ValueError("batchSize must be positive")

    self.options = options
```

### 2. Handle Errors Gracefully

```python
def read(self, partition):
    max_retries = 3
    retry_delay = 1.0

    for attempt in range(max_retries):
        try:
            # Attempt to fetch data
            data = self._fetch_data_from_api()
            for record in data:
                yield self._transform_record(record)
            return  # Success, exit

        except ConnectionError as e:
            if attempt < max_retries - 1:
                print(f"Connection failed, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                # Final attempt failed
                if self.options.get("failOnError", "true").lower() == "true":
                    raise
                else:
                    print(f"Skipping partition due to error: {e}")
                    return  # Return empty iterator

def _fetch_data_from_api(self):
    """Fetch data with proper error handling."""
    import requests

    response = requests.get(
        self.api_url,
        timeout=self.timeout,
        headers={"Authorization": f"Bearer {self.api_key}"}
    )
    response.raise_for_status()  # Raise on HTTP errors
    return response.json()
```

### 3. Make Classes Serializable

```python
class SerializableReader(DataSourceReader):
    def __init__(self, options, schema):
        # ✅ Store only serializable data
        self.connection_string = options.get("connectionString")
        self.query = options.get("query")
        self.schema = schema

        # ❌ Don't store non-serializable objects
        # self.connection = psycopg2.connect(connection_string)
        # self.http_client = requests.Session()

    def read(self, partition):
        # Create non-serializable objects locally in read()
        import psycopg2

        conn = psycopg2.connect(self.connection_string)
        try:
            cursor = conn.cursor()
            cursor.execute(self.query)

            for row in cursor:
                yield row
        finally:
            conn.close()
```

### 4. Implement Resource Management

```python
class ResourceManagedReader(DataSourceStreamReader):
    def __init__(self, options, schema):
        self.options = options
        self.schema = schema
        self._resources = []

    def _acquire_resource(self, resource_id):
        """Track resources for cleanup."""
        resource = self._create_resource(resource_id)
        self._resources.append(resource)
        return resource

    def stop(self):
        """Clean up all resources when streaming stops."""
        for resource in self._resources:
            try:
                resource.close()
            except Exception as e:
                print(f"Error closing resource: {e}")

        self._resources.clear()
        print("All resources cleaned up")

    def __del__(self):
        """Ensure cleanup even if stop() isn't called."""
        if self._resources:
            self.stop()
```

### 5. Optimize for Performance

```python
class PerformantReader(DataSourceReader):
    def read(self, partition):
        # Use batch fetching instead of row-by-row
        batch_size = 1000
        offset = 0

        while True:
            # Fetch batch of records
            batch = self._fetch_batch(offset, batch_size)
            if not batch:
                break

            # Use generator for memory efficiency
            for record in batch:
                yield record

            offset += batch_size

    def _fetch_batch(self, offset, limit):
        """Fetch a batch of records efficiently."""
        # Example: Use SQL LIMIT/OFFSET
        query = f"SELECT * FROM table LIMIT {limit} OFFSET {offset}"
        return self._execute_query(query)
```

## Common Patterns

### Reading from REST APIs

```python
class RestApiReader(DataSourceReader):
    def __init__(self, options, schema):
        self.base_url = options.get("url")
        self.auth_token = options.get("authToken")
        self.page_size = int(options.get("pageSize", "100"))
        self.schema = schema

    def read(self, partition):
        import requests

        headers = {"Authorization": f"Bearer {self.auth_token}"} if self.auth_token else {}
        page = 1

        while True:
            response = requests.get(
                f"{self.base_url}?page={page}&size={self.page_size}",
                headers=headers
            )
            response.raise_for_status()

            data = response.json()
            if not data.get("items"):
                break

            for item in data["items"]:
                yield self._transform_to_row(item)

            if not data.get("hasMore", False):
                break

            page += 1
```

### Reading from Databases

```python
class DatabaseReader(DataSourceReader):
    def __init__(self, options, schema):
        self.jdbc_url = options.get("url")
        self.table = options.get("table")
        self.username = options.get("username")
        self.password = options.get("password")
        self.schema = schema

    def partitions(self):
        """Partition by ID ranges for parallel reads."""
        import jaydebeapi

        conn = self._get_connection()
        cursor = conn.cursor()

        # Get min and max IDs
        cursor.execute(f"SELECT MIN(id), MAX(id) FROM {self.table}")
        min_id, max_id = cursor.fetchone()
        conn.close()

        # Create partitions
        num_partitions = 4
        range_size = (max_id - min_id + 1) // num_partitions

        partitions = []
        for i in range(num_partitions):
            start = min_id + (i * range_size)
            end = start + range_size if i < num_partitions - 1 else max_id + 1
            partitions.append(InputPartition({"start": start, "end": end}))

        return partitions

    def read(self, partition):
        conn = self._get_connection()
        cursor = conn.cursor()

        bounds = partition.value
        query = f"""
            SELECT * FROM {self.table}
            WHERE id >= {bounds['start']} AND id < {bounds['end']}
        """

        cursor.execute(query)
        for row in cursor:
            yield row

        conn.close()

    def _get_connection(self):
        import jaydebeapi
        return jaydebeapi.connect(
            "com.mysql.jdbc.Driver",
            self.jdbc_url,
            [self.username, self.password]
        )
```

### Handling Authentication

```python
class AuthenticatedDataSource(DataSource):
    def __init__(self, options):
        # Handle different authentication methods
        if "api_key" in options:
            self.auth_method = "api_key"
            self.credentials = options["api_key"]
        elif "oauth_token" in options:
            self.auth_method = "oauth"
            self.credentials = options["oauth_token"]
        elif "username" in options and "password" in options:
            self.auth_method = "basic"
            self.credentials = (options["username"], options["password"])
        else:
            raise ValueError("No valid authentication method provided")

        self.options = options

    def reader(self, schema):
        return AuthenticatedReader(self.options, self.auth_method, self.credentials, schema)
```

## Debugging and Testing

### Enable Debug Logging

```python
class DebuggableReader(DataSourceReader):
    def __init__(self, options, schema):
        self.options = options
        self.schema = schema
        self.debug = options.get("debug", "false").lower() == "true"

    def read(self, partition):
        if self.debug:
            print(f"Starting read for partition: {partition.value if hasattr(partition, 'value') else 'single'}")
            print(f"Options: {self.options}")
            print(f"Schema: {self.schema}")

        count = 0
        for record in self._read_data():
            if self.debug and count < 5:
                print(f"Record {count}: {record}")

            yield record
            count += 1

        if self.debug:
            print(f"Finished reading {count} records")
```

### Unit Testing Your Data Source

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("test") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_basic_read(spark):
    # Register your data source
    spark.dataSource.register(MyCustomDataSource)

    # Read data
    df = spark.read.format("mycustom") \
        .option("numRows", "10") \
        .load()

    # Verify results
    assert df.count() == 10
    assert len(df.columns) == 3
    assert df.schema == expected_schema

def test_partitioned_read(spark):
    spark.dataSource.register(MyCustomDataSource)

    df = spark.read.format("mycustom") \
        .option("numPartitions", "4") \
        .load()

    # Check partitioning
    assert df.rdd.getNumPartitions() == 4

    # Verify data integrity
    assert df.select("id").distinct().count() == df.count()

def test_error_handling(spark):
    spark.dataSource.register(MyCustomDataSource)

    # Test missing required option
    with pytest.raises(ValueError, match="api_key is required"):
        spark.read.format("mycustom").load()

def test_streaming(spark):
    spark.dataSource.register(MyStreamingDataSource)

    # Start streaming query
    query = spark.readStream.format("mystreaming") \
        .load() \
        .writeStream \
        .format("memory") \
        .queryName("test_stream") \
        .start()

    # Wait for some data
    query.processAllAvailable()

    # Verify streamed data
    result = spark.table("test_stream")
    assert result.count() > 0

    query.stop()
```

## Next Steps

- Review the [API Reference](api-reference.md) for complete method signatures
- Study the [example data sources](data-sources-guide.md) for real-world patterns
- Check the [Development Guide](../contributing/DEVELOPMENT.md) for testing and debugging tips

Remember that building good data sources is iterative. Start simple, test thoroughly, and gradually add features like partitioning, streaming, and optimization as needed.
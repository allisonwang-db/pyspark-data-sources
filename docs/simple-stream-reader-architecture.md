# SimpleDataSourceStreamReader Architecture

## Overview

`SimpleDataSourceStreamReader` is a lightweight streaming data source reader in PySpark designed for scenarios with small data volumes and low throughput requirements. Unlike the standard `DataSourceStreamReader`, it executes entirely on the driver node, trading scalability for simplicity.

## Key Architecture Components

### Python-Side Components

#### SimpleDataSourceStreamReader (datasource.py)
The user-facing API with three core methods:
- `initialOffset()`: Returns the starting position for a new streaming query
- `read(start)`: Reads all available data from a given offset and returns both the data and the next offset
- `readBetweenOffsets(start, end)`: Re-reads data deterministically for failure recovery

#### _SimpleStreamReaderWrapper (datasource_internal.py)
A private wrapper that implements the prefetch-and-cache pattern:
- Maintains `current_offset` to track reading progress
- Caches prefetched data in memory on the driver
- Converts simple reader interface to standard streaming reader interface

### Scala-Side Components

#### PythonMicroBatchStream (PythonMicroBatchStream.scala)
Manages the micro-batch execution:
- Creates and manages `PythonStreamingSourceRunner` for Python communication
- Stores prefetched data in BlockManager with `PythonStreamBlockId`
- Handles offset management and partition planning

#### PythonStreamingSourceRunner (PythonStreamingSourceRunner.scala)
The bridge between JVM and Python:
- Spawns a Python worker process running `python_streaming_source_runner.py`
- Serializes/deserializes data using Arrow format
- Manages RPC-style communication for method invocations

## Data Flow and Lifecycle

### Query Initialization
1. Spark creates `PythonMicroBatchStream` when a streaming query starts
2. `PythonStreamingSourceRunner` spawns a Python worker process
3. Python worker instantiates the `SimpleDataSourceStreamReader`
4. Initial offset is obtained via `initialOffset()` call

### Micro-batch Execution (per trigger)

#### 1. Offset Discovery (Driver)
- Spark calls `latestOffset()` on `PythonMicroBatchStream`
- Runner invokes Python's `latestOffset()` via RPC
- Wrapper calls `simple_reader.read(current_offset)` to prefetch data
- Data and new offset are returned and cached

#### 2. Data Caching (Driver)
- Prefetched records are converted to Arrow batches
- Data is stored in BlockManager with a unique `PythonStreamBlockId`
- Cache entry maintains mapping of (start_offset, end_offset) → data

#### 3. Partition Planning (Driver)
- `planInputPartitions(start, end)` creates a single `PythonStreamingInputPartition`
- Partition references the cached block ID
- No actual data distribution happens (single partition on driver)

#### 4. Data Reading (Executor)
- Executor retrieves cached data from BlockManager using block ID
- Data is already in Arrow format for efficient processing
- Records are converted to internal rows for downstream processing

## Integration with Spark Structured Streaming APIs

### User API Integration

```python
# User defines a SimpleDataSourceStreamReader
class MyStreamReader(SimpleDataSourceStreamReader):
    def initialOffset(self):
        return {"position": 0}

    def read(self, start):
        # Read data from source
        data = fetch_data_since(start["position"])
        new_offset = {"position": start["position"] + len(data)}
        return (iter(data), new_offset)

    def readBetweenOffsets(self, start, end):
        # Re-read for failure recovery
        return fetch_data_between(start["position"], end["position"])

# Register and use with Spark
class MyDataSource(DataSource):
    def simpleStreamReader(self, schema):
        return MyStreamReader()

spark.dataSource.register(MyDataSource)
df = spark.readStream.format("my_source").load()
query = df.writeStream.format("console").start()
```

### Streaming Engine Integration

1. **Trigger Processing**: Works with all trigger modes (ProcessingTime, Once, AvailableNow)
2. **Offset Management**: Offsets are checkpointed to WAL for exactly-once semantics
3. **Failure Recovery**: Uses `readBetweenOffsets()` to replay uncommitted batches
4. **Commit Protocol**: After successful batch, `commit(offset)` is called for cleanup

## Execution Flow Diagram

```
Driver Node                          Python Worker                    Executors
-----------                          -------------                    ---------
PythonMicroBatchStream
    |
    ├─> latestOffset() ──────────> PythonStreamingSourceRunner
    |                                      |
    |                                      ├─> RPC: LATEST_OFFSET ──> SimpleStreamReaderWrapper
    |                                      |                              |
    |                                      |                              ├─> read(current_offset)
    |                                      |                              |    └─> (data, new_offset)
    |                                      |                              |
    |                                      |<── Arrow batches ────────────┘
    |                                      |
    ├─> Cache in BlockManager <───────────┘
    |   (PythonStreamBlockId)
    |
    ├─> planInputPartitions()
    |   └─> Single partition with BlockId
    |
    └─> createReaderFactory() ─────────────────────────────────────> Read from BlockManager
                                                                          |
                                                                          └─> Process records
```

## Key Design Decisions and Trade-offs

### Advantages
- **Simplicity**: No need to implement partitioning logic
- **Consistency**: All data reading happens in one place (driver)
- **Efficiency for small data**: Avoids overhead of distributed execution
- **Easy offset management**: Single reader maintains consistent view of progress
- **Quick development**: Minimal boilerplate for simple streaming sources

### Limitations
- **Not scalable**: All data flows through driver (bottleneck)
- **Memory constraints**: Driver must cache entire micro-batch
- **Single point of failure**: Driver failure affects data reading
- **Network overhead**: Data must be transferred from driver to executors
- **Throughput ceiling**: Limited by driver's processing capacity

### Important Note from Source Code
From datasource.py:
> "Because SimpleDataSourceStreamReader read records in Spark driver node to determine end offset of each batch without partitioning, it is only supposed to be used in lightweight use cases where input rate and batch size is small."

## Use Cases

### Ideal for:
- Configuration change streams
- Small lookup table updates
- Low-volume event streams (< 1000 records/sec)
- Prototyping and testing streaming applications
- REST API polling with low frequency
- File system monitoring for small files
- Message queue consumers with low throughput

### Not suitable for:
- High-throughput data sources (use `DataSourceStreamReader` instead)
- Large batch sizes that exceed driver memory
- Sources requiring parallel reads for performance
- Production workloads with high availability requirements
- Kafka topics with high message rates
- Large file streaming

## Implementation Example: File Monitor

```python
import os
import json
from typing import Iterator, Tuple, Dict
from pyspark.sql.datasource import SimpleDataSourceStreamReader

class FileMonitorStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, path: str):
        self.path = path

    def initialOffset(self) -> Dict:
        # Start with empty file list
        return {"processed_files": []}

    def read(self, start: Dict) -> Tuple[Iterator[Tuple], Dict]:
        processed = set(start.get("processed_files", []))
        current_files = set(os.listdir(self.path))
        new_files = current_files - processed

        # Read content from new files
        data = []
        for filename in new_files:
            filepath = os.path.join(self.path, filename)
            if os.path.isfile(filepath):
                with open(filepath, 'r') as f:
                    content = f.read()
                    data.append((filename, content))

        # Update offset
        new_offset = {"processed_files": list(current_files)}

        return (iter(data), new_offset)

    def readBetweenOffsets(self, start: Dict, end: Dict) -> Iterator[Tuple]:
        # For recovery: re-read files that were added between start and end
        start_files = set(start.get("processed_files", []))
        end_files = set(end.get("processed_files", []))
        files_to_read = end_files - start_files

        data = []
        for filename in files_to_read:
            filepath = os.path.join(self.path, filename)
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    content = f.read()
                    data.append((filename, content))

        return iter(data)
```

## Performance Considerations

### Memory Management
- Driver memory should be configured to handle maximum expected batch size
- Use `spark.driver.memory` and `spark.driver.maxResultSize` appropriately
- Monitor driver memory usage during streaming

### Optimization Tips
1. **Batch Size Control**: Implement rate limiting in `read()` method
2. **Data Compression**: Use efficient serialization formats (Parquet, Arrow)
3. **Offset Design**: Keep offset structure simple and small
4. **Caching Strategy**: Clear old cache entries in `commit()` method
5. **Error Handling**: Implement robust error handling in read methods

### Monitoring
Key metrics to monitor:
- Driver memory usage
- Batch processing time
- Records per batch
- Offset checkpoint frequency
- Cache hit/miss ratio

## Comparison with DataSourceStreamReader

| Feature | SimpleDataSourceStreamReader | DataSourceStreamReader |
|---------|------------------------------|------------------------|
| Execution Location | Driver only | Distributed (executors) |
| Partitioning | Single partition | Multiple partitions |
| Scalability | Limited | High |
| Implementation Complexity | Low | High |
| Memory Requirements | Driver-heavy | Distributed |
| Suitable Data Volume | < 10MB per batch | Unlimited |
| Parallelism | None | Configurable |
| Use Case | Prototyping, small streams | Production, large streams |

## Conclusion

`SimpleDataSourceStreamReader` provides an elegant solution for integrating small-scale streaming data sources with Spark's Structured Streaming engine. By executing entirely on the driver, it simplifies development while maintaining full compatibility with Spark's streaming semantics. However, users must carefully consider the scalability limitations and ensure their use case fits within the architectural constraints of driver-side execution.

For production systems with high throughput requirements, the standard `DataSourceStreamReader` with proper partitioning should be used instead.
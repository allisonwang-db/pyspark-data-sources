# PySpark Data Sources - Project Context for Claude

## Project Overview
This is a demonstration library showcasing custom Spark data sources built using Apache Spark 4.0's new Python Data Source API. The project provides various data source connectors for reading from external APIs and services.

**Important**: This is a demo/educational project and not intended for production use.

## Tech Stack
- **Language**: Python (3.9-3.12)
- **Framework**: Apache Spark 4.0+ (PySpark)
- **Package Management**: Poetry
- **Documentation**: MkDocs with Material theme
- **Testing**: pytest
- **Dependencies**: PyArrow, requests, faker, and optional extras

## Project Structure
```
pyspark_datasources/
├── __init__.py          # Main package exports
├── fake.py             # Fake data generator using Faker
├── github.py           # GitHub repository data connector
├── googlesheets.py     # Public Google Sheets reader
├── huggingface.py      # Hugging Face datasets connector
├── kaggle.py           # Kaggle datasets connector
├── lance.py            # Lance vector database connector
├── opensky.py          # OpenSky flight data connector
├── simplejson.py       # JSON writer for Databricks DBFS
├── stock.py            # Alpha Vantage stock data reader
└── weather.py          # Weather data connector
```

## Available Data Sources
| Short Name    | File | Description | Dependencies |
|---------------|------|-------------|--------------|
| `fake`        | fake.py | Generate fake data using Faker | faker |
| `github`      | github.py | Read GitHub repository PRs | None |
| `googlesheets`| googlesheets.py | Read public Google Sheets | None |
| `huggingface` | huggingface.py | Access Hugging Face datasets | datasets |
| `kaggle`      | kaggle.py | Read Kaggle datasets | kagglehub, pandas |
| `opensky`     | opensky.py | Flight data from OpenSky Network | None |
| `simplejson`  | simplejson.py | Write JSON to Databricks DBFS | databricks-sdk |
| `stock`       | stock.py | Stock data from Alpha Vantage | None |

## Development Commands

### Environment Setup
```bash
poetry install                    # Install dependencies
poetry install --extras all      # Install all optional dependencies
poetry shell                     # Activate virtual environment
```

### Testing
```bash
# Note: On macOS, set this environment variable to avoid fork safety issues
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

pytest                              # Run all tests
pytest tests/test_data_sources.py  # Run specific test file
pytest tests/test_data_sources.py::test_arrow_datasource_single_file -v # Run a specific test
```

### Documentation
```bash
mkdocs serve                     # Start local docs server
mkdocs build                     # Build static documentation
```

### Code Formatting
This project uses [Ruff](https://github.com/astral-sh/ruff) for code formatting and linting.

```bash
poetry run ruff format .         # Format code
poetry run ruff check .          # Run linter
poetry run ruff check . --fix    # Run linter with auto-fix
```

### Package Management
Please refer to RELEASE.md for more details.
```bash
poetry build                     # Build package
poetry publish                   # Publish to PyPI (requires auth)
poetry add <package>             # Add new dependency
poetry update                    # Update dependencies
```

## Usage Patterns
All data sources follow the Spark Data Source API pattern:

```python
from pyspark_datasources import FakeDataSource

# Register the data source
spark.dataSource.register(FakeDataSource)

# Batch reading
df = spark.read.format("fake").option("numRows", 100).load()

# Streaming (where supported)
stream = spark.readStream.format("fake").load()
```

## Testing Strategy
- Tests use pytest with PySpark session fixtures
- Each data source has basic functionality tests
- Tests verify data reading and schema validation
- Some tests may require external API access

## Key Implementation Details
- All data sources inherit from Spark's DataSource base class
- Implements reader() method for batch reading
- Some implement streamReader() for streaming
- Schema is defined using PySpark StructType
- Options are passed via Spark's option() method

## External Dependencies
- **GitHub API**: Uses public API, no auth required
- **Alpha Vantage**: Stock data API (may require API key)
- **Google Sheets**: Public sheets only, no auth
- **Kaggle**: Requires Kaggle API credentials
- **Databricks**: SDK for DBFS access
- **OpenSky**: Public flight data API

## Common Issues
- Ensure PySpark >= 4.0.0 is installed
- Some data sources require API keys/credentials
- Network connectivity required for external APIs
- Rate limiting may affect some external services

## Python Data Source API Specification

### Core Abstract Base Classes

#### DataSource
Primary abstract base class for custom data sources supporting read/write operations.

**Key Methods:**
- `__init__(self, options: Dict[str, str])` - Initialize with user options (Optional; base class provides default)
- `name() -> str` - Return format name (Optional to override; defaults to class name)
- `schema() -> StructType` - Define data source schema (Required)
- `reader(schema: StructType) -> DataSourceReader` - Create batch reader (Required if batch read is supported)
- `writer(schema: StructType, overwrite: bool) -> DataSourceWriter` - Create batch writer (Required if batch write is supported)
- `streamReader(schema: StructType) -> DataSourceStreamReader` - Create streaming reader (Required if streaming read is supported and `simpleStreamReader` is not implemented)
- `streamWriter(schema: StructType, overwrite: bool) -> DataSourceStreamWriter` - Create streaming writer (Required if streaming write is supported)
- `simpleStreamReader(schema: StructType) -> SimpleDataSourceStreamReader` - Create simple streaming reader (Required if streaming read is supported and `streamReader` is not implemented)

#### DataSourceReader
Abstract base class for reading data from sources.

**Key Methods:**
- `read(partition) -> Iterator` - Read data from partition, returns tuples/Rows/pyarrow.RecordBatch (Required)
- `partitions() -> List[InputPartition]` - Return input partitions for parallel reading (Optional; defaults to a single partition)

#### DataSourceStreamReader
Abstract base class for streaming data sources with offset management.

**Key Methods:**
- `initialOffset() -> dict` - Return starting offset (Required)
- `latestOffset() -> dict` - Return latest available offset (Required)
- `partitions(start: dict, end: dict) -> List[InputPartition]` - Get partitions for offset range (Required)
- `read(partition) -> Iterator` - Read data from partition (Required)
- `commit(end: dict) -> None` - Mark offsets as processed (Optional)
- `stop() -> None` - Clean up resources (Optional)

#### SimpleDataSourceStreamReader
Simplified streaming reader interface without partition planning.

**Key Methods:**
- `initialOffset() -> dict` - Return starting offset (Required)
- `read(start: dict) -> Tuple[Iterator, dict]` - Read from start offset; return an iterator and the next start offset (Required)
- `readBetweenOffsets(start: dict, end: dict) -> Iterator` - Deterministic replay between offsets for recovery (Optional; recommended for reliable recovery)
- `commit(end: dict) -> None` - Mark offsets as processed (Optional)

#### DataSourceWriter
Abstract base class for writing data to external sources.

**Key Methods:**
- `write(iterator) -> WriteResult` - Write data from iterator (Required)
- `abort(messages: List[WriterCommitMessage])` - Handle write failures (Optional)
- `commit(messages: List[WriterCommitMessage]) -> WriteResult` - Commit successful writes (Required)

#### DataSourceStreamWriter
Abstract base class for writing data to external sinks in streaming queries.

**Key Methods:**
- `write(iterator) -> WriterCommitMessage` - Write data for a partition and return a commit message (Required)
- `commit(messages: List[WriterCommitMessage], batchId: int) -> None` - Commit successful microbatch writes (Required)
- `abort(messages: List[WriterCommitMessage], batchId: int) -> None` - Handle write failures for a microbatch (Optional)

#### DataSourceArrowWriter
Optimized writer using PyArrow RecordBatch for improved performance.

### Implementation Requirements

1. **Serialization**: All classes must be pickle serializable
2. **Schema Definition**: Use PySpark StructType for schema specification
3. **Data Types**: Support standard Spark SQL data types
4. **Error Handling**: Implement proper exception handling
5. **Resource Management**: Clean up resources properly in streaming sources
6. **Use load() for paths**: Specify file paths in `load("/path")`, not `option("path", "/path")`

### Usage Patterns

```python
# Custom data source implementation
class MyDataSource(DataSource):
    def __init__(self, options):
        self.options = options
    
    def name(self):
        return "myformat"
    
    def schema(self):
        return StructType([StructField("id", IntegerType(), True)])
    
    def reader(self, schema):
        return MyDataSourceReader(self.options, schema)

# Registration and usage
spark.dataSource.register(MyDataSource)
df = spark.read.format("myformat").option("key", "value").load()
```

### Performance Optimizations

1. **Arrow Integration**: Return `pyarrow.RecordBatch` for better serialization
2. **Partitioning**: Implement `partitions()` for parallel processing
3. **Lazy Evaluation**: Defer expensive operations until read time

## Documentation
- Main docs: https://allisonwang-db.github.io/pyspark-data-sources/
- Individual data source docs in `docs/datasources/`
- Spark Data Source API: https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html
- API Source Code: https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py

### Data Source Docstring Guidelines
When creating new data sources, include these sections in the class docstring:

**Required Sections:**
- Brief description and `Name: "format_name"`
- `Options` section documenting all parameters with types/defaults
- `Examples` section with registration and basic usage

**Key Guidelines:**
- **Include schema output**: Show `df.printSchema()` results for clarity
- **Document error cases**: Show what happens with missing files/invalid options
- **Document partitioning strategy**: Show how data sources leverage partitioning to increase performance
- **Document Arrow optimization**: Show how data data sources use Arrow to transmit data

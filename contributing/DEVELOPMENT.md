# Development Guide

## Environment Setup

### Prerequisites
- Python 3.9-3.12
- Poetry for dependency management
- Apache Spark 4.0+ (or Databricks Runtime 15.4 LTS+)

### Installation

```bash
# Clone the repository
git clone https://github.com/allisonwang-db/pyspark-data-sources.git
cd pyspark-data-sources

# Install dependencies
poetry install

# Install with all optional dependencies
poetry install --extras all

# Activate virtual environment
poetry shell
```

### macOS Setup
On macOS, you may encounter fork safety issues with PyArrow. Set this environment variable:

```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

# Add to your shell profile for persistence
echo 'export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES' >> ~/.zshrc
```

## Testing

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_data_sources.py

# Run specific test with verbose output
pytest tests/test_data_sources.py::test_fake_datasource -v

# Run with coverage
pytest --cov=pyspark_datasources --cov-report=html
```

### Writing Tests

Tests follow this pattern:

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("test") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_my_datasource(spark):
    from pyspark_datasources import MyDataSource
    spark.dataSource.register(MyDataSource)

    df = spark.read.format("myformat").load()
    assert df.count() > 0
    assert len(df.columns) == expected_columns
```

## Code Quality

### Formatting with Ruff

This project uses [Ruff](https://github.com/astral-sh/ruff) for code formatting and linting.

```bash
# Format code
poetry run ruff format .

# Run linter
poetry run ruff check .

# Run linter with auto-fix
poetry run ruff check . --fix

# Check specific file
poetry run ruff check pyspark_datasources/fake.py
```

### Pre-commit Hooks (Optional)

```bash
# Install pre-commit hooks
poetry add --group dev pre-commit
pre-commit install

# Run manually
pre-commit run --all-files
```

## Documentation

### Building Documentation

The project previously used MkDocs for documentation. Documentation now lives primarily in:
- README.md - Main documentation
- Docstrings in source code
- Contributing guides in /contributing

### Writing Docstrings

Follow this pattern for data source docstrings:

```python
class MyDataSource(DataSource):
    """
    Brief description of the data source.

    Longer description explaining what it does and any important details.

    Name: `myformat`

    Options
    -------
    option1 : str, optional
        Description of option1 (default: "value")
    option2 : int, required
        Description of option2

    Examples
    --------
    Register and use the data source:

    >>> from pyspark_datasources import MyDataSource
    >>> spark.dataSource.register(MyDataSource)
    >>> df = spark.read.format("myformat").option("option2", 100).load()
    >>> df.show()
    +---+-----+
    | id|value|
    +---+-----+
    |  1|  foo|
    |  2|  bar|
    +---+-----+

    >>> df.printSchema()
    root
     |-- id: integer (nullable = true)
     |-- value: string (nullable = true)
    """
```

## Adding New Data Sources

### Step 1: Create the Data Source File

Create a new file in `pyspark_datasources/`:

```python
# pyspark_datasources/mynewsource.py
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType

class MyNewDataSource(DataSource):
    def name(self):
        return "mynewformat"

    def schema(self):
        return StructType([
            StructField("field1", StringType(), True),
            StructField("field2", StringType(), True)
        ])

    def reader(self, schema):
        return MyNewReader(self.options, schema)

class MyNewReader(DataSourceReader):
    def __init__(self, options, schema):
        self.options = options
        self.schema = schema

    def read(self, partition):
        # Implement reading logic
        for i in range(10):
            yield ("value1", "value2")
```

### Step 2: Add to __init__.py

```python
# pyspark_datasources/__init__.py
from pyspark_datasources.mynewsource import MyNewDataSource

__all__ = [
    # ... existing exports ...
    "MyNewDataSource",
]
```

### Step 3: Add Tests

```python
# tests/test_data_sources.py
def test_mynew_datasource(spark):
    from pyspark_datasources import MyNewDataSource
    spark.dataSource.register(MyNewDataSource)

    df = spark.read.format("mynewformat").load()
    assert df.count() == 10
    assert df.columns == ["field1", "field2"]
```

### Step 4: Update Documentation

Add your data source to the table in README.md with examples.

## Package Management

### Adding Dependencies

```bash
# Add required dependency
poetry add requests

# Add optional dependency
poetry add --optional faker

# Add dev dependency
poetry add --group dev pytest-cov

# Update dependencies
poetry update
```

### Managing Extras

Edit `pyproject.toml` to add optional dependency groups:

```toml
[tool.poetry.extras]
mynewsource = ["special-library"]
all = ["faker", "datasets", "special-library", ...]
```

## Debugging

### Enable Spark Logging

```python
import logging
logging.basicConfig(level=logging.INFO)

# Or in Spark config
spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.log.level", "INFO") \
    .getOrCreate()
```

### Debug Data Source

```python
class DebugReader(DataSourceReader):
    def read(self, partition):
        print(f"Reading partition: {partition.value if hasattr(partition, 'value') else partition}")
        print(f"Options: {self.options}")

        # Your reading logic
        for row in data:
            print(f"Yielding: {row}")
            yield row
```

### Common Issues

1. **Serialization errors**: Ensure all class attributes are pickle-able
2. **Schema mismatch**: Verify returned data matches declared schema
3. **Missing dependencies**: Use try/except to provide helpful error messages
4. **API rate limits**: Implement backoff and retry logic

## Performance Optimization

### Use Partitioning

```python
class OptimizedReader(DataSourceReader):
    def partitions(self):
        # Split work into multiple partitions
        num_partitions = int(self.options.get("numPartitions", "4"))
        return [InputPartition(i) for i in range(num_partitions)]

    def read(self, partition):
        # Each partition processes its subset
        partition_id = partition.value
        # Process only this partition's data
```

### Use Arrow Format

```python
import pyarrow as pa

class ArrowOptimizedReader(DataSourceReader):
    def read(self, partition):
        # Return pyarrow.RecordBatch for better performance
        arrays = [
            pa.array(["value1", "value2"]),
            pa.array([1, 2])
        ]
        batch = pa.RecordBatch.from_arrays(arrays, names=["col1", "col2"])
        yield batch
```

## Continuous Integration

The project uses GitHub Actions for CI/CD. Workflows are defined in `.github/workflows/`.

### Running CI Locally

```bash
# Install act (GitHub Actions locally)
brew install act  # macOS

# Run workflows
act -j test
```

## Troubleshooting

### PyArrow Issues on macOS

```bash
# Set environment variable
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES

# Or in Python
import os
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
```

### Poetry Issues

```bash
# Clear cache
poetry cache clear pypi --all

# Update lock file
poetry lock --no-update

# Reinstall
poetry install --remove-untracked
```

### Spark Session Issues

```python
# Stop existing session
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
if spark:
    spark.stop()

# Create new session
spark = SparkSession.builder.appName("debug").getOrCreate()
```
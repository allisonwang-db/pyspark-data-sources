# PySpark Data Sources

[![pypi](https://img.shields.io/pypi/v/pyspark-data-sources.svg?color=blue)](https://pypi.org/project/pyspark-data-sources/)
[![code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Custom Apache Spark data sources using the [Python Data Source API](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html) (Spark 4.0+). Learn by example and build your own data sources.

## Quick Start

### Installation

```bash
pip install pyspark-data-sources

# Install with specific extras
pip install pyspark-data-sources[faker]        # For FakeDataSource

pip install pyspark-data-sources[all]          # All optional dependencies
```

### Requirements
- Apache Spark 4.0+ or [Databricks Runtime 15.4 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/15.4lts)+
- Python 3.9-3.12

### Basic Usage

```python
from pyspark.sql import SparkSession
from pyspark_datasources import FakeDataSource

# Create Spark session
spark = SparkSession.builder.appName("datasource-demo").getOrCreate()

# Register the data source
spark.dataSource.register(FakeDataSource)

# Read batch data
df = spark.read.format("fake").option("numRows", 5).load()
df.show()
# +--------------+----------+-------+------------+
# |          name|      date|zipcode|       state|
# +--------------+----------+-------+------------+
# |  Pam Mitchell|1988-10-20|  23788|   Tennessee|
# |Melissa Turner|1996-06-14|  30851|      Nevada|
# |  Brian Ramsey|2021-08-21|  55277|  Washington|
# |  Caitlin Reed|1983-06-22|  89813|Pennsylvania|
# | Douglas James|2007-01-18|  46226|     Alabama|
# +--------------+----------+-------+------------+

# Stream data
stream = spark.readStream.format("fake").load()
query = stream.writeStream.format("console").start()
```

## Available Data Sources

### Sources (Read)

| Data Source | Type | Description | Dependency |
|-------------|------|-------------|------------|
| `fake` | Batch/Stream | Generate synthetic test data using Faker | `[faker]` |
| `github` | Batch | Read GitHub pull requests | None |
| `googlesheets` | Batch | Read public Google Sheets | None |
| `huggingface` | Batch | Load Hugging Face datasets | `[huggingface]` |
| `stock` | Batch | Fetch stock market data (Alpha Vantage) | None |
| `opensky` | Batch/Stream | Live flight tracking data | None |
| `kaggle` | Batch | Load Kaggle datasets | `[kaggle]` |
| `arrow` | Batch | Read Apache Arrow files | `[arrow]` |
| `robinhood` | Batch | Read cryptocurrency market data from Robinhood API | `[robinhood]` |
| `jsonplaceholder` | Batch | Read JSON data for testing | None |
| `weather` | Batch | Read current weather data (OpenWeatherMap) | None |

### Sinks (Write)

| Data Source | Type | Description | Dependency |
|-------------|------|-------------|------------|
| `lance` | Batch Write | Write Lance vector format | `[lance]` |
| `salesforce` | Stream Write | Write to Salesforce objects | `[salesforce]` |
| `meta_capi` | Batch/Stream Write | Write to Meta Conversions API | None |


📚 **[See detailed examples for all data sources →](docs/data-sources-guide.md)**

## Example: Generate Fake Data

```python
from pyspark_datasources import FakeDataSource

spark.dataSource.register(FakeDataSource)

# Generate synthetic data with custom schema
df = spark.read.format("fake") \
    .schema("name string, email string, company string") \
    .option("numRows", 5) \
    .load()

df.show(truncate=False)
# +------------------+-------------------------+-----------------+
# |name              |email                    |company          |
# +------------------+-------------------------+-----------------+
# |Christine Sampson |johnsonjeremy@example.com|Hernandez-Nguyen |
# |Yolanda Brown     |williamlowe@example.net  |Miller-Hernandez |
# +------------------+-------------------------+-----------------+
```

## Building Your Own Data Source

Here's a minimal example to get started:

```python
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class MyCustomDataSource(DataSource):
    def name(self):
        return "mycustom"

    def schema(self):
        return StructType([
            StructField("id", IntegerType()),
            StructField("name", StringType())
        ])

    def reader(self, schema):
        return MyCustomReader(self.options, schema)

class MyCustomReader(DataSourceReader):
    def __init__(self, options, schema):
        self.options = options
        self.schema = schema

    def read(self, partition):
        # Your data reading logic here
        for i in range(10):
            yield (i, f"name_{i}")

# Register and use
spark.dataSource.register(MyCustomDataSource)
df = spark.read.format("mycustom").load()
```

📖 **[Complete guide with advanced patterns →](docs/building-data-sources.md)**

## Documentation

- 📚 **[Data Sources Guide](docs/data-sources-guide.md)** - Detailed examples for each data source
- 🔧 **[Building Data Sources](docs/building-data-sources.md)** - Complete tutorial with advanced patterns
- 📖 **[API Reference](docs/api-reference.md)** - Full API specification and method signatures
- 💻 **[Development Guide](contributing/DEVELOPMENT.md)** - Contributing and development setup

## Requirements

- Apache Spark 4.0+ or Databricks Runtime 15.4 LTS+
- Python 3.9-3.12

## Contributing

We welcome contributions! See our [Development Guide](contributing/DEVELOPMENT.md) for details.

## Resources

- [Python Data Source API Documentation](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html)
- [API Source Code](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py)

## Community Tools
- [Polymo](https://github.com/dan1elt0m/polymo) - Declarative REST API ingestion. [Medium article](https://medium.com/@d.e.tom89/turn-any-rest-api-into-spark-dataframes-in-minutes-with-polymo-028a48113eb1)
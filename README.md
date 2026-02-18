# PySpark Data Sources

[![pypi](https://img.shields.io/pypi/v/pyspark-data-sources.svg?color=blue)](https://pypi.org/project/pyspark-data-sources/)
[![code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Custom Apache Spark data sources using the [Python Data Source API](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html) (Spark 4.0+). Requires Apache Spark 4.0+ or [Databricks Runtime 15.4 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/15.4lts)+.

## Quick Start

### Installation

```bash
pip install pyspark-data-sources
```

### Basic Usage

```python
from pyspark.sql import SparkSession
from pyspark_datasources import GithubDataSource

spark = SparkSession.builder.appName("datasource-demo").getOrCreate()
spark.dataSource.register(GithubDataSource)

# Read pull requests from a public GitHub repository (no API key required)
df = spark.read.format("github").load("apache/spark")
df.select("id", "title", "author").show()
# +---+--------------------+--------+
# | id|               title|  author|
# +---+--------------------+--------+
# |  1|Initial commit      |  matei |...
# +---+--------------------+--------+
```

## Available Data Sources

| Data Source | Read | Write | Description | Installation | Example |
|-------------|------|-------|-------------|--------------|--------|
| `arrow` | Batch | — | Read Apache Arrow files | built-in | [→](examples/arrow.md) |
| `fake` | Batch, Stream | — | Generate synthetic test data using Faker | built-in | [→](examples/fake.md) |
| `github` | Batch | — | Read GitHub pull requests | built-in | [→](examples/github.md) |
| `googlesheets` | Batch | — | Read public Google Sheets | built-in | [→](examples/googlesheets.md) |
| `huggingface` | Batch | — | Load Hugging Face datasets | `pip install pyspark-data-sources[datasets]` | [→](examples/huggingface.md) |
| `jira` | Batch | Batch | Read and write Jira issues | `pip install pyspark-data-sources[jira]` | [→](examples/jira.md) |
| `kaggle` | Batch | — | Load Kaggle datasets | `pip install pyspark-data-sources[kaggle]` | [→](examples/kaggle.md) |
| `lance` | — | Batch | Write Lance vector format | `pip install pyspark-data-sources[lance]` | [→](examples/lance.md) |
| `meta_capi` | — | Batch, Stream | Write to Meta Conversions API | built-in | [→](examples/meta_capi.md) |
| `notion` | Batch | Batch | Read and write Notion databases | `pip install pyspark-data-sources[notion]` | [→](examples/notion.md) |
| `opensky` | Batch, Stream | — | Live flight tracking data | built-in | [→](examples/opensky.md) |
| `robinhood` | Batch | — | Cryptocurrency market data from Robinhood API | `pip install pyspark-data-sources[robinhood]` | [→](examples/robinhood.md) |
| `salesforce` | — | Stream | Write to Salesforce objects | `pip install pyspark-data-sources[salesforce]` | [→](examples/salesforce.md) |
| `sftp` | Batch | Batch | Read/write files from SFTP server | `pip install pyspark-data-sources[sftp]` | [→](examples/sftp.md) |
| `oracle` | Batch | Batch | Read/write Oracle databases | `pip install pyspark-data-sources[oracledb]` | [→](examples/oracle.md) |
| `stock` | Batch | — | Fetch stock market data (Alpha Vantage) | built-in | [→](examples/stock.md) |
| `bored` | Batch | — | Activity suggestions from Bored API | built-in | [→](examples/bored.md) |
| `cocktail` | Batch | — | Cocktail recipes from TheCocktailDB | built-in | [→](examples/cocktail.md) |
| `dog` | Batch | — | Dog breeds from Dog CEO API | built-in | [→](examples/dog.md) |
| `httpbin` | Batch | — | Test data from httpbin.org | built-in | [→](examples/httpbin.md) |
| `ipapi` | Batch | — | IP geolocation from ip-api.com | built-in | [→](examples/ipapi.md) |
| `jsonlines` | Batch | — | Read .jsonl files | built-in | [→](examples/jsonlines.md) |
| `quotable` | Batch | — | Quotes from Quotable API | built-in | [→](examples/quotable.md) |
| `tomlfile` | Batch | — | Read TOML files | `pip install pyspark-data-sources[toml]` | [→](examples/tomlfile.md) |
| `universities` | Batch | — | University data from HipoLabs | built-in | [→](examples/universities.md) |
| `yamlfile` | Batch | — | Read YAML files | `pip install pyspark-data-sources[yaml]` | [→](examples/yamlfile.md) |
| `jsonplaceholder` | Batch | — | Read JSON data for testing | built-in | [→](examples/jsonplaceholder.md) |
| `weather` | Batch | — | Read current weather data (OpenWeatherMap) | built-in | [→](examples/weather.md) |

📚 **[See detailed examples →](docs/data-sources-guide.md)** · **[Copy-pastable examples →](examples/README.md)**

## Building Your Own Data Source

**Recommendation:** Leverage AI to speed up development. If you use [Cursor](https://cursor.com), try the `create-data-source` skill (in `.cursor/skills/`) to generate a full implementation with templates and a step-by-step checklist.

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

- 📚 **[Data Sources Guide](docs/data-sources-guide.md)** - Common patterns and troubleshooting
- 🔧 **[Building Data Sources](docs/building-data-sources.md)** - Complete tutorial with advanced patterns
- 📖 **[API Reference](docs/api-reference.md)** - Full API specification and method signatures
- 💻 **[Development Guide](contributing/DEVELOPMENT.md)** - Contributing and development setup

## Contributing

We welcome contributions! See our [Development Guide](contributing/DEVELOPMENT.md) for details.

## Resources

- [Python Data Source API Documentation](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html)
- [API Source Code](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py)

## Community Tools
- [Polymo](https://github.com/dan1elt0m/polymo) - Declarative REST API ingestion. [Medium article](https://medium.com/@d.e.tom89/turn-any-rest-api-into-spark-dataframes-in-minutes-with-polymo-028a48113eb1)
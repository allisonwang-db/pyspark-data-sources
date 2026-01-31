# Data Sources Guide

This guide provides detailed examples and usage patterns for all available data sources in the PySpark Data Sources library.

## Table of Contents
1. [FakeDataSource - Generate Synthetic Data](#1-fakedatasource---generate-synthetic-data)
2. [GitHubDataSource - Read GitHub Pull Requests](#2-githubdatasource---read-github-pull-requests)
3. [GoogleSheetsDataSource - Read Public Google Sheets](#3-googlesheetsdatasource---read-public-google-sheets)
4. [HuggingFaceDataSource - Load Datasets from Hugging Face Hub](#4-huggingfacedatasource---load-datasets-from-hugging-face-hub)
5. [StockDataSource - Fetch Stock Market Data](#5-stockdatasource---fetch-stock-market-data)
6. [OpenSkyDataSource - Stream Live Flight Data](#6-openskydatasource---stream-live-flight-data)
7. [KaggleDataSource - Load Kaggle Datasets](#7-kaggledatasource---load-kaggle-datasets)
8. [ArrowDataSource - Read Apache Arrow Files](#8-arrowdatasource---read-apache-arrow-files)
9. [LanceDataSource - Vector Database Format](#9-lancedatasource---vector-database-format)
10. [SFTPDataSource - Read/Write SFTP Files](#10-sftpdatasource---readwrite-sftp-files)

## 1. FakeDataSource - Generate Synthetic Data

Generate fake data using the Faker library for testing and development.

### Installation
```bash
pip install pyspark-data-sources[faker]
```

### Basic Usage
```python
from pyspark_datasources import FakeDataSource

spark.dataSource.register(FakeDataSource)

# Basic usage with default schema
df = spark.read.format("fake").load()
df.show()
# +-----------+----------+-------+-------+
# |       name|      date|zipcode|  state|
# +-----------+----------+-------+-------+
# |Carlos Cobb|2018-07-15|  73003|Indiana|
# | Eric Scott|1991-08-22|  10085|  Idaho|
# | Amy Martin|1988-10-28|  68076| Oregon|
# +-----------+----------+-------+-------+
```

### Custom Schema
Field names must match Faker provider methods. See [Faker documentation](https://faker.readthedocs.io/en/master/providers.html) for available providers.

```python
# Custom schema - field names must match Faker provider methods
df = spark.read.format("fake") \
    .schema("name string, email string, phone_number string, company string") \
    .option("numRows", 5) \
    .load()
df.show(truncate=False)
# +------------------+-------------------------+----------------+-----------------+
# |name              |email                    |phone_number    |company          |
# +------------------+-------------------------+----------------+-----------------+
# |Christine Sampson |johnsonjeremy@example.com|+1-673-684-4608 |Hernandez-Nguyen |
# |Yolanda Brown     |williamlowe@example.net  |(262)562-4152   |Miller-Hernandez |
# |Joshua Hernandez  |mary38@example.com       |7785366623      |Davis Group      |
# |Joseph Gallagher  |katiepatterson@example.net|+1-648-619-0997 |Brown-Fleming    |
# |Tina Morrison     |johnbell@example.com     |(684)329-3298   |Sherman PLC      |
# +------------------+-------------------------+----------------+-----------------+
```

### Streaming Usage
```python
# Streaming usage - generates continuous data
stream = spark.readStream.format("fake") \
    .option("rowsPerMicrobatch", 10) \
    .load()

query = stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()
```

## 2. GitHubDataSource - Read GitHub Pull Requests

Fetch pull request data from GitHub repositories using the public GitHub API.

### Basic Usage
```python
from pyspark_datasources import GithubDataSource

spark.dataSource.register(GithubDataSource)

# Read pull requests from a public repository
df = spark.read.format("github").load("apache/spark")
df.select("number", "title", "state", "user", "created_at").show(truncate=False)
# +------+--------------------------------------------+------+---------------+--------------------+
# |number|title                                       |state |user           |created_at          |
# +------+--------------------------------------------+------+---------------+--------------------+
# |48730 |[SPARK-49998] Fix JSON parsing regression  |open  |john_doe       |2024-11-20T10:15:30Z|
# |48729 |[SPARK-49997] Improve error messages       |merged|contributor123 |2024-11-19T15:22:45Z|
# +------+--------------------------------------------+------+---------------+--------------------+

# Schema of the GitHub data source
df.printSchema()
# root
#  |-- number: integer (nullable = true)
#  |-- title: string (nullable = true)
#  |-- state: string (nullable = true)
#  |-- user: string (nullable = true)
#  |-- created_at: string (nullable = true)
#  |-- updated_at: string (nullable = true)
```

### Notes
- Uses the public GitHub API (rate limited to 60 requests/hour for unauthenticated requests)
- Returns the most recent pull requests
- Repository format: "owner/repository"

## 3. GoogleSheetsDataSource - Read Public Google Sheets

Read data from publicly accessible Google Sheets.

### Basic Usage
```python
from pyspark_datasources import GoogleSheetsDataSource

spark.dataSource.register(GoogleSheetsDataSource)

# Read from a public Google Sheet
sheet_url = "https://docs.google.com/spreadsheets/d/1H7bKPGpAXbPRhTYFxqg1h6FmKl5ZhCTM_5OlAqCHfVs"
df = spark.read.format("googlesheets").load(sheet_url)
df.show()
# +------+-------+--------+
# |Name  |Age    |City    |
# +------+-------+--------+
# |Alice |25     |NYC     |
# |Bob   |30     |LA      |
# |Carol |28     |Chicago |
# +------+-------+--------+
```

### Specify Sheet Name or Index
```python
# Read a specific sheet by name
df = spark.read.format("googlesheets") \
    .option("sheetName", "Sheet2") \
    .load(sheet_url)

# Or by index (0-based)
df = spark.read.format("googlesheets") \
    .option("sheetIndex", "1") \
    .load(sheet_url)
```

### Requirements
- Sheet must be publicly accessible (anyone with link can view)
- First row is used as column headers
- Data is read as strings by default

## 4. HuggingFaceDataSource - Load Datasets from Hugging Face Hub

Access thousands of datasets from the Hugging Face Hub.

### Installation
```bash
pip install pyspark-data-sources[huggingface]
```

### Basic Usage
```python
from pyspark_datasources import HuggingFaceDataSource

spark.dataSource.register(HuggingFaceDataSource)

# Load a dataset from Hugging Face
df = spark.read.format("huggingface").load("imdb")
df.select("text", "label").show(2, truncate=False)
# +--------------------------------------------------+-----+
# |text                                              |label|
# +--------------------------------------------------+-----+
# |I rented this movie last night and I must say... |0    |
# |This film is absolutely fantastic! The acting... |1    |
# +--------------------------------------------------+-----+
```

### Advanced Options
```python
# Load specific split
df = spark.read.format("huggingface") \
    .option("split", "train") \
    .load("squad")

# Load dataset with configuration
df = spark.read.format("huggingface") \
    .option("config", "plain_text") \
    .load("wikipedia")

# Load specific subset
df = spark.read.format("huggingface") \
    .option("split", "validation") \
    .option("config", "en") \
    .load("wikipedia")
```

### Performance Tips
- The data source automatically partitions large datasets for parallel processing
- First load may be slow as it downloads and caches the dataset
- Subsequent reads use the cached data

## 5. StockDataSource - Fetch Stock Market Data

Get stock market data from Alpha Vantage API.

### Setup
Obtain a free API key from [Alpha Vantage](https://www.alphavantage.co/support/#api-key).

### Basic Usage
```python
from pyspark_datasources import StockDataSource

spark.dataSource.register(StockDataSource)

# Fetch stock data (requires Alpha Vantage API key)
df = spark.read.format("stock") \
    .option("symbols", "AAPL,GOOGL,MSFT") \
    .option("api_key", "YOUR_API_KEY") \
    .load()

df.show()
# +------+----------+------+------+------+------+--------+
# |symbol|timestamp |open  |high  |low   |close |volume  |
# +------+----------+------+------+------+------+--------+
# |AAPL  |2024-11-20|175.50|178.25|175.00|177.80|52341200|
# |GOOGL |2024-11-20|142.30|143.90|141.50|143.25|18234500|
# |MSFT  |2024-11-20|425.10|428.75|424.50|427.90|12456300|
# +------+----------+------+------+------+------+--------+

# Schema
df.printSchema()
# root
#  |-- symbol: string (nullable = true)
#  |-- timestamp: string (nullable = true)
#  |-- open: double (nullable = true)
#  |-- high: double (nullable = true)
#  |-- low: double (nullable = true)
#  |-- close: double (nullable = true)
#  |-- volume: long (nullable = true)
```

### Options
- `symbols`: Comma-separated list of stock symbols
- `api_key`: Your Alpha Vantage API key
- `function`: Time series function (default: "TIME_SERIES_DAILY")

## 6. OpenSkyDataSource - Stream Live Flight Data

Stream real-time flight tracking data from OpenSky Network.

### Streaming Usage
```python
from pyspark_datasources import OpenSkyDataSource

spark.dataSource.register(OpenSkyDataSource)

# Stream flight data for a specific region
stream = spark.readStream.format("opensky") \
    .option("region", "EUROPE") \
    .load()

# Process the stream
query = stream.select("icao24", "callsign", "origin_country", "longitude", "latitude", "altitude") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

# Sample output:
# +------+--------+--------------+---------+--------+--------+
# |icao24|callsign|origin_country|longitude|latitude|altitude|
# +------+--------+--------------+---------+--------+--------+
# |4b1806|SWR123  |Switzerland   |8.5123   |47.3765 |10058.4 |
# |3c6750|DLH456  |Germany       |13.4050  |52.5200 |11887.2 |
# +------+--------+--------------+---------+--------+--------+
```

### Batch Usage
```python
# Read current flight data as batch
df = spark.read.format("opensky") \
    .option("region", "USA") \
    .load()

df.count()  # Number of flights currently in USA airspace
```

### Region Options
- `EUROPE`: European airspace
- `USA`: United States airspace
- `ASIA`: Asian airspace
- `WORLD`: Global (all flights)

## 7. KaggleDataSource - Load Kaggle Datasets

Download and read datasets from Kaggle.

### Setup
1. Install Kaggle API: `pip install pyspark-data-sources[kaggle]`
2. Get API credentials from https://www.kaggle.com/account
3. Place credentials in `~/.kaggle/kaggle.json`

### Basic Usage
```python
from pyspark_datasources import KaggleDataSource

spark.dataSource.register(KaggleDataSource)

# Load a Kaggle dataset
df = spark.read.format("kaggle").load("titanic")
df.select("PassengerId", "Name", "Age", "Survived").show(5)
# +-----------+--------------------+----+--------+
# |PassengerId|Name                |Age |Survived|
# +-----------+--------------------+----+--------+
# |1          |Braund, Mr. Owen    |22.0|0       |
# |2          |Cumings, Mrs. John  |38.0|1       |
# |3          |Heikkinen, Miss Laina|26.0|1       |
# |4          |Futrelle, Mrs Jacques|35.0|1       |
# |5          |Allen, Mr. William  |35.0|0       |
# +-----------+--------------------+----+--------+
```

### Multi-file Datasets
```python
# Load specific file from multi-file dataset
df = spark.read.format("kaggle") \
    .option("file", "train.csv") \
    .load("competitionname/datasetname")

# Load all CSV files
df = spark.read.format("kaggle") \
    .option("pattern", "*.csv") \
    .load("multi-file-dataset")
```

## 8. ArrowDataSource - Read Apache Arrow Files

Efficiently read Arrow format files with zero-copy operations.

### Installation
```bash
pip install pyspark-data-sources[arrow]
```

### Usage
```python
from pyspark_datasources import ArrowDataSource

spark.dataSource.register(ArrowDataSource)

# Read Arrow files
df = spark.read.format("arrow").load("/path/to/data.arrow")

# Read multiple Arrow files
df = spark.read.format("arrow").load("/path/to/arrow/files/*.arrow")

# The Arrow reader is optimized for performance
df.count()  # Fast counting due to Arrow metadata

# Schema is preserved from Arrow files
df.printSchema()
```

### Performance Benefits
- Zero-copy reads when possible
- Preserves Arrow metadata
- Efficient columnar data access
- Automatic schema inference

## 9. LanceDataSource - Vector Database Format

Read and write Lance format data, optimized for vector/ML workloads.

### Installation
```bash
pip install pyspark-data-sources[lance]
```

### Write Data
```python
from pyspark_datasources import LanceDataSource

spark.dataSource.register(LanceDataSource)

# Prepare your DataFrame
df = spark.range(1000).selectExpr(
    "id",
    "array(rand(), rand(), rand()) as vector",
    "rand() as score"
)

# Write DataFrame to Lance format
df.write.format("lance") \
    .mode("overwrite") \
    .save("/path/to/lance/dataset")
```

### Read Data
```python
# Read Lance dataset
lance_df = spark.read.format("lance") \
    .load("/path/to/lance/dataset")

lance_df.show(5)

# Lance preserves vector columns efficiently
lance_df.printSchema()
# root
#  |-- id: long (nullable = false)
#  |-- vector: array (nullable = true)
#  |    |-- element: double (containsNull = true)
#  |-- score: double (nullable = true)
```

### Features
- Optimized for vector/embedding data
- Efficient storage of array columns
- Fast random access
- Version control built-in

## 10. SFTPDataSource - Read/Write SFTP Files

Read and write text files to an SFTP server.

### Installation
```bash
pip install pyspark-data-sources[sftp]
```

### Read Data
```python
from pyspark_datasources import SFTPDataSource

spark.dataSource.register(SFTPDataSource)

# Read from SFTP
df = spark.read.format("sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "user") \
    .option("password", "pass") \
    .option("path", "/remote/path/data.txt") \
    .load()
```

### Write Data
```python
# Write to SFTP
df.write.format("sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "user") \
    .option("password", "pass") \
    .option("path", "/remote/path/output") \
    .save()
```

### Options
- `host`: SFTP server hostname (required)
- `username`: SFTP username (required)
- `password`: SFTP password (optional if key_filename provided)
- `key_filename`: Path to private key file (optional if password provided)
- `path`: Remote file or directory path (required)
- `port`: SFTP port (default: 22)
- `recursive`: Recursively list files in directories (read only, default: false)

## Common Patterns

### Error Handling
Most data sources will raise informative errors when required options are missing:

```python
# This will raise an error about missing API key
df = spark.read.format("stock") \
    .option("symbols", "AAPL") \
    .load()
# ValueError: api_key option is required for StockDataSource
```

### Schema Inference vs Specification
Some data sources support both automatic schema inference and explicit schema specification:

```python
# Automatic schema inference
df = spark.read.format("fake").load()

# Explicit schema specification
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType())
])

df = spark.read.format("fake") \
    .schema(schema) \
    .load()
```

### Partitioning for Performance
Many data sources automatically partition data for parallel processing:

```python
# HuggingFace automatically partitions large datasets
df = spark.read.format("huggingface").load("wikipedia")
df.rdd.getNumPartitions()  # Returns number of partitions

# You can control partitioning in some data sources
df = spark.read.format("fake") \
    .option("numPartitions", "8") \
    .load()
```

## Troubleshooting

### Common Issues

1. **Missing Dependencies**
   ```bash
   # Install specific extras
   pip install pyspark-data-sources[faker,huggingface,kaggle]
   ```

2. **API Rate Limits**
   - GitHub: 60 requests/hour (unauthenticated)
   - Alpha Vantage: 5 requests/minute (free tier)
   - Use caching or implement retry logic

3. **Network Issues**
   - Most data sources require internet access
   - Check firewall/proxy settings
   - Some sources support offline mode after initial cache

4. **Memory Issues with Large Datasets**
   - Use partitioning for large datasets
   - Consider sampling: `df.sample(0.1)`
   - Increase Spark executor memory
For more help, see the [Development Guide](../contributing/DEVELOPMENT.md) or open an issue on GitHub.

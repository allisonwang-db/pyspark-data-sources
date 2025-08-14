# PySpark Data Sources

[![pypi](https://img.shields.io/pypi/v/pyspark-data-sources.svg?color=blue)](https://pypi.org/project/pyspark-data-sources/)
[![code style: ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This repository showcases custom Spark data sources built using the new [**Python Data Source API**](https://spark.apache.org/docs/4.0.0/api/python/tutorial/sql/python_data_source.html) introduced in Apache Spark 4.0.
For an in-depth understanding of the API, please refer to the [API source code](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py).
Note this repo is demo only and please be aware that it is not intended for production use.
Contributions and feedback are welcome to help improve the examples.


## Installation
```
pip install pyspark-data-sources
```

## Usage
Make sure you have pyspark >= 4.0.0 installed. 

```
pip install pyspark
```

Or use [Databricks Runtime 15.4 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/15.4lts) or above versions, or [Databricks Serverless](https://docs.databricks.com/aws/en/compute/serverless/).


```python
from pyspark_datasources.fake import FakeDataSource

# Register the data source
spark.dataSource.register(FakeDataSource)

spark.read.format("fake").load().show()

# For streaming data generation
spark.readStream.format("fake").load().writeStream.format("console").start()
```

## Example Data Sources

| Data Source                                                             | Short Name     | Type           | Description                                   | Dependencies          | Example                                                                                                                                                                      |
|-------------------------------------------------------------------------|----------------|----------------|-----------------------------------------------|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Batch Read** | | | | | |
| [ArrowDataSource](pyspark_datasources/arrow.py)                        | `arrow`        | Batch Read     | Read Apache Arrow files (.arrow)             | `pyarrow`             | `pip install pyspark-data-sources[arrow]`<br/>`spark.read.format("arrow").load("/path/to/file.arrow")`                                                                                                     |
| [FakeDataSource](pyspark_datasources/fake.py)                          | `fake`         | Batch/Streaming Read | Generate fake data using the `Faker` library | `faker`               | `pip install pyspark-data-sources[fake]`<br/>`spark.read.format("fake").load()` or `spark.readStream.format("fake").load()`                                                                                |
| [GithubDataSource](pyspark_datasources/github.py)                      | `github`       | Batch Read     | Read pull requests from a Github repository  | None                  | `pip install pyspark-data-sources`<br/>`spark.read.format("github").load("apache/spark")`                                                                                                                 |
| [GoogleSheetsDataSource](pyspark_datasources/googlesheets.py)          | `googlesheets` | Batch Read     | Read table from public Google Sheets        | None                  | `pip install pyspark-data-sources`<br/>`spark.read.format("googlesheets").load("https://docs.google.com/spreadsheets/d/...")`                                                                             |
| [HuggingFaceDatasets](pyspark_datasources/huggingface.py)              | `huggingface`  | Batch Read     | Read datasets from HuggingFace Hub           | `datasets`            | `pip install pyspark-data-sources[huggingface]`<br/>`spark.read.format("huggingface").load("imdb")`                                                                                                         |
| [KaggleDataSource](pyspark_datasources/kaggle.py)                      | `kaggle`       | Batch Read     | Read datasets from Kaggle                    | `kagglehub`, `pandas` | `pip install pyspark-data-sources[kaggle]`<br/>`spark.read.format("kaggle").load("titanic")`                                                                                                               |
| [StockDataSource](pyspark_datasources/stock.py)                        | `stock`        | Batch Read     | Read stock data from Alpha Vantage           | None                  | `pip install pyspark-data-sources`<br/>`spark.read.format("stock").option("symbols", "AAPL,GOOGL").option("api_key", "key").load()`                                                                  |
| **Batch Write** | | | | | |
| [LanceSink](pyspark_datasources/lance.py)                              | `lance`        | Batch Write    | Write data in Lance format                    | `lance`               | `pip install pyspark-data-sources[lance]`<br/>`df.write.format("lance").mode("append").save("/tmp/lance_data")`                                                                                          |
| [SimpleJsonDataSource](pyspark_datasources/simplejson.py)              | `simplejson`   | Batch Write    | Write JSON data to Databricks DBFS           | `databricks-sdk`      | `pip install pyspark-data-sources[simplejson]`<br/>`df.write.format("simplejson").save("/path/to/output")`                                                                                               |
| **Streaming Read** | | | | | |
| [OpenSkyDataSource](pyspark_datasources/opensky.py)                 | `opensky`      | Streaming Read | Read from OpenSky Network.                   | None                  | `pip install pyspark-data-sources`<br/>`spark.readStream.format("opensky").option("region", "EUROPE").load()`                                                                                            |
| [WeatherDataSource](pyspark_datasources/weather.py)                    | `weather`      | Streaming Read | Fetch weather data from tomorrow.io           | None                  | `pip install pyspark-data-sources`<br/>`spark.readStream.format("weather").option("locations", "[(37.7749, -122.4194)]").option("apikey", "key").load()`                                          |
| **Streaming Write** | | | | | |
| [SalesforceDataSource](pyspark_datasources/salesforce.py)              | `pyspark.datasource.salesforce`   | Streaming Write | Streaming datasource for writing data to Salesforce | `simple-salesforce`   | `pip install pyspark-data-sources[salesforce]`<br/>`df.writeStream.format("pyspark.datasource.salesforce").option("username", "user").start()`                                                         |

See more here: https://allisonwang-db.github.io/pyspark-data-sources/.

## Official Data Sources

For production use, consider these official data source implementations built with the Python Data Source API:

| Data Source              | Repository                                                                                    | Description                                              | Features                                                                                                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------|----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| **HuggingFace Datasets** | [@huggingface/pyspark_huggingface](https://github.com/huggingface/pyspark_huggingface)       | Production-ready Spark Data Source for 🤗 Hugging Face Datasets | • Stream datasets as Spark DataFrames<br>• Select subsets/splits with filters<br>• Authentication support<br>• Save DataFrames to Hugging Face<br> |

## Data Source Naming Convention

When creating custom data sources using the Python Data Source API, follow these naming conventions for the `short_name` parameter:

### Recommended Approach
- **Use the system name directly**: Use lowercase system names like `huggingface`, `opensky`, `googlesheets`, etc.
- This provides clear, intuitive naming that matches the service being integrated

### Conflict Resolution
- **If there's a naming conflict**: Use the format `pyspark.datasource.<system_name>`
- Example: `pyspark.datasource.salesforce` if "salesforce" conflicts with existing naming

### Examples from this repository:
```python
# Direct system naming (preferred)
spark.read.format("github").load()       # GithubDataSource
spark.read.format("googlesheets").load() # GoogleSheetsDataSource  
spark.read.format("opensky").load()      # OpenSkyDataSource

# Namespaced format (when conflicts exist)
spark.read.format("pyspark.datasource.opensky").load()
```

## Contributing
We welcome and appreciate any contributions to enhance and expand the custom data sources.:

- **Add New Data Sources**: Want to add a new data source using the Python Data Source API? Submit a pull request or open an issue.
- **Suggest Enhancements**: If you have ideas to improve a data source or the API, we'd love to hear them!
- **Report Bugs**: Found something that doesn't work as expected? Let us know by opening an issue.


## Development
### Environment Setup
```
poetry install
poetry env activate
```

### Build Docs
```
mkdocs serve
```

### Code Formatting
This project uses [Ruff](https://github.com/astral-sh/ruff) for code formatting and linting.

```bash
# Format code
poetry run ruff format .

# Run linter
poetry run ruff check .

# Run linter with auto-fix
poetry run ruff check . --fix
```

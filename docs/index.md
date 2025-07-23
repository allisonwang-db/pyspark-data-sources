# PySpark Data Sources

Custom Spark data sources for reading and writing data in Apache Spark, using the Python Data Source API.

## Installation

```bash
pip install pyspark-data-sources
```

If you want to install all extra dependencies, use:

```bash
pip install pyspark-data-sources[all]
```

## Usage

```python
from pyspark_datasources.fake import FakeDataSource

# Register the data source
spark.dataSource.register(FakeDataSource)

spark.read.format("fake").load().show()

# For streaming data generation
spark.readStream.format("fake").load().writeStream.format("console").start()
```


## Data Sources

| Data Source                                             | Short Name        | Description                                   | Dependencies          |
|---------------------------------------------------------|-------------------|-----------------------------------------------|-----------------------|
| [GithubDataSource](./datasources/github.md)             | `github`          | Read pull requests from a Github repository   | None                  |
| [FakeDataSource](./datasources/fake.md)                 | `fake`            | Generate fake data using the `Faker` library  | `faker`               |
| [HuggingFaceDatasets](./datasources/huggingface.md)     | `huggingface`     | Read datasets from the HuggingFace Hub        | `datasets`            |
| [StockDataSource](./datasources/stock.md)               | `stock`           | Read stock data from Alpha Vantage            | None                  |
| [SimpleJsonDataSource](./datasources/simplejson.md)     | `simplejson`      | Write JSON data to Databricks DBFS            | `databricks-sdk`      |
| [GoogleSheetsDataSource](./datasources/googlesheets.md) | `googlesheets`    | Read table from public Google Sheets document | None                  |
| [KaggleDataSource](./datasources/kaggle.md)             | `kaggle`          | Read datasets from Kaggle                     | `kagglehub`, `pandas` |
| [JSONPlaceHolder](./datasources/jsonplaceholder.md)     | `jsonplaceholder` | Read JSON data for testing and prototyping    | None                  |

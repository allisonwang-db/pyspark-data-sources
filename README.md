# PySpark Data Sources

[![pypi](https://img.shields.io/pypi/v/pyspark-data-sources.svg?color=blue)](https://pypi.org/project/pyspark-data-sources/)

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

| Data Source                                                             | Short Name     | Description                                   | Dependencies          |
|-------------------------------------------------------------------------|----------------|-----------------------------------------------|-----------------------|
| [GithubDataSource](pyspark_datasources/github.py)                      | `github`       | Read pull requests from a Github repository  | None                  |
| [FakeDataSource](pyspark_datasources/fake.py)                          | `fake`         | Generate fake data using the `Faker` library | `faker`               |
| [StockDataSource](pyspark_datasources/stock.py)                        | `stock`        | Read stock data from Alpha Vantage           | None                  |
| [GoogleSheetsDataSource](pyspark_datasources/googlesheets.py)          | `googlesheets` | Read table from public Google Sheets        | None                  |
| [KaggleDataSource](pyspark_datasources/kaggle.py)                      | `kaggle`       | Read datasets from Kaggle                    | `kagglehub`, `pandas` |
| [SimpleJsonDataSource](pyspark_datasources/simplejson.py)              | `simplejson`   | Write JSON data to Databricks DBFS                 | `databricks-sdk`      |
| [OpenSkyDataSource](pyspark_datasources/opensky.py)                 | `opensky`      | Read from OpenSky Network.                   | None                  |

See more here: https://allisonwang-db.github.io/pyspark-data-sources/.

## Official Data Sources

For production use, consider these official data source implementations built with the Python Data Source API:

| Data Source              | Repository                                                                                    | Description                                              | Features                                                                                                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------|----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| **HuggingFace Datasets** | [@huggingface/pyspark_huggingface](https://github.com/huggingface/pyspark_huggingface)       | Production-ready Spark Data Source for 🤗 Hugging Face Datasets | • Stream datasets as Spark DataFrames<br>• Select subsets/splits with filters<br>• Authentication support<br>• Save DataFrames to Hugging Face<br> |

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

# PySpark Data Sources

Custom Spark data sources for reading and writing data in Apache Spark, using the Python Data Source API.

## Installation

```bash
pip install pyspark-data-sources
```

## Usage

```python
from pyspark_datasources.github import GithubDataSource

# Register the data source
spark.dataSource.register(GithubDataSource)

spark.read.format("github").load("apache/spark").show()
```


## Data Sources

| Data Source                                 | Short Name | Description                                   |
|---------------------------------------------|------------|-----------------------------------------------|
| [GithubDataSource](./datasources/github.md) | `github`   | Read pull requests from a Github repository   |
| [FakeDataSource](./datasources/fake.md)     | `fake`     | Generate fake data using the `Faker` library  |


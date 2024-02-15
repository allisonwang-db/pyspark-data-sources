# PySpark Data Sources

Custom Spark data sources for reading and writing data in Apache Spark, using the Python Data Source API.

## Installation

```bash
pip install pyspark-data-sources
```

If you want to install one of the extra dependencies, for example `datasets`, use

```bash
pip install pyspark-data-sources[datasets]
```

If you want to install all extra dependencies, use:

```bash
pip install pyspark-data-sources[all]
```

## Usage

```python
from pyspark_datasources import GithubDataSource

# Register the data source
spark.dataSource.register(GithubDataSource)

spark.read.format("github").load("apache/spark").show()
```


## Data Sources

| Data Source                                         | Short Name    | Description                                   | Dependencies   |
|-----------------------------------------------------|---------------|-----------------------------------------------|----------------|
| [GithubDataSource](./datasources/github.md)         | `github`      | Read pull requests from a Github repository   | None           |
| [FakeDataSource](./datasources/fake.md)             | `fake`        | Generate fake data using the `Faker` library  | `faker`        |
| [HuggingFaceDatasets](./datasources/huggingface.md) | `huggingface` | Read datasets from the HuggingFace Hub        | `datasets`     |

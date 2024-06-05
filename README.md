# pyspark-data-sources

[![pypi](https://img.shields.io/pypi/v/pyspark-data-sources.svg?color=blue)](https://pypi.org/project/pyspark-data-sources/)

This repository showcases custom Spark data sources built using the new [**Python Data Source API**](https://issues.apache.org/jira/browse/SPARK-44076) for the upcoming Apache Spark 4.0 release.
For an in-depth understanding of the API, please refer to the [API source code](https://github.com/apache/spark/blob/master/python/pyspark/sql/datasource.py).
Note this repo is **demo only** and please be aware that it is not intended for production use.
Contributions and feedback are welcome to help improve the examples.


## Installation
```
pip install pyspark-data-sources[all]
```

## Usage

Install the pyspark 4.0 preview version: https://pypi.org/project/pyspark/4.0.0.dev1/

```
pip install "pyspark[connect]==4.0.0.dev1"
```

Or use Databricks Runtime 15.2 or above.

Try the data sources!

```python
from pyspark_datasources.github import GithubDataSource

# Register the data source
spark.dataSource.register(GithubDataSource)

spark.read.format("github").load("apache/spark").show()
```

See more here: https://allisonwang-db.github.io/pyspark-data-sources/.

## Contributing
We welcome and appreciate any contributions to enhance and expand the custom data sources. If you're interested in contributing:

- **Add New Data Sources**: Want to add a new data source using the Python Data Source API? Submit a pull request or open an issue.
- **Suggest Enhancements**: If you have ideas to improve a data source or the API, we'd love to hear them!
- **Report Bugs**: Found something that doesn't work as expected? Let us know by opening an issue.

**Need help or have questions?** Don't hesitate to open a new issue, and we'll do our best to assist you.

## Development

### Build docs
```
mkdocs serve
```

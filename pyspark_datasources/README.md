## Developing Locally

### Init Project
This repo uses [Poetry](https://python-poetry.org/docs/#installation).

```
poetry install
```

### Install PySpark from the latest Spark master:
- Clone the Apache Spark repo: `git clone git@github.com:apache/spark.git`
- Build Spark: `build/sbt clean package`
- Build PySpark: `cd python && python setup.py sdist`
- Install PySpark: `poetry run pip install <path-to-spark-repo>/python/dist/pyspark-4.0.0.dev0.tar.gz`

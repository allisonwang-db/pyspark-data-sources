# PyPI Data Source Example

Read Python package metadata from PyPI. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import PyPiDataSource

spark = SparkSession.builder.appName("pypi-example").getOrCreate()
spark.dataSource.register(PyPiDataSource)
```

### Step 2: Read Package Info

```python
df = spark.read.format("pypi").load("requests")
df.select("package_name", "version", "summary", "author").show(truncate=50)
```

### Step 3: Read Multiple Packages (in a loop)

```python
packages = ["requests", "numpy", "pandas"]
dfs = [spark.read.format("pypi").load(p) for p in packages]
combined = dfs[0]
for df in dfs[1:]:
    combined = combined.union(df)
combined.show()
```

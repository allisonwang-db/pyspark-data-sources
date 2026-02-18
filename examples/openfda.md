# OpenFDA Data Source Example

Read drug labels, food recalls, and device events from the FDA's OpenFDA API. No API key required for reasonable usage.

## Setup Credentials

None required (optional API key for higher rate limits).

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import OpenFdaDataSource

spark = SparkSession.builder.appName("openfda-example").getOrCreate()
spark.dataSource.register(OpenFdaDataSource)
```

### Step 2: Read Drug Labels (Default)

```python
df = spark.read.format("openfda").option("limit", "50").load()
df.select("brand_name", "generic_name", "manufacturer_name", "purpose").show(10, truncate=30)
```

### Step 3: Read Food Recalls

```python
df = spark.read.format("openfda").option("endpoint", "food/event").option("limit", "20").load()
df.show(5, truncate=30)
```

### Step 4: Filter by Manufacturer

```python
df = spark.read.format("openfda").option("limit", "200").load()
top_manufacturers = df.filter("manufacturer_name != ''").groupBy("manufacturer_name").count()
top_manufacturers.orderBy("count", ascending=False).show(10, truncate=30)
```

# Random User Data Source Example

Read fake user profiles from the Random User API. No credentials required. Useful for testing and demos.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import RandomUserDataSource

spark = SparkSession.builder.appName("randomuser-example").getOrCreate()
spark.dataSource.register(RandomUserDataSource)
```

### Step 2: Read Default (10 Users)

```python
df = spark.read.format("randomuser").load()
df.select("first_name", "last_name", "email", "country").show(10, truncate=False)
```

### Step 3: Read More Users

```python
df = spark.read.format("randomuser").option("results", "100").load()
df.count()
```

### Step 4: Filter and Aggregate

```python
df = spark.read.format("randomuser").option("results", "50").load()
by_country = df.groupBy("country").count().orderBy("count", ascending=False)
by_country.show(10, truncate=False)
```

### Example Output

```
+-------------+-----+
|country      |count|
+-------------+-----+
|United States|   12|
|United Kingdom|   8|
|France       |   6|
+-------------+-----+
```

# REST Countries Data Source Example

Read country data from the free REST Countries API. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import RestCountriesDataSource

spark = SparkSession.builder.appName("restcountries-example").getOrCreate()
spark.dataSource.register(RestCountriesDataSource)
```

### Step 2: Read All Countries

```python
df = spark.read.format("restcountries").load()
df.select("name_common", "capital", "region", "population").show(10, truncate=False)
```

### Step 3: Read Specific Country by Name

```python
df = spark.read.format("restcountries").load("germany")
df.show()
```

### Step 4: Filter and Aggregate

```python
df = spark.read.format("restcountries").load()
top_pop = df.orderBy(df.population.desc()).limit(10)
top_pop.select("name_common", "population", "area").show(truncate=False)
```

### Example Output

```
+------------------+----------+--------+
|name_common        |population|area    |
+------------------+----------+--------+
|India             |1417173173|3287263.0|
|China             |1425887337|9640011.0|
|United States     |329484123 |9833520.0|
+------------------+----------+--------+
```

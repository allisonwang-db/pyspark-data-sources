# Agify API Data Source Example

Age prediction from first name via api.agify.io. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import AgifyDataSource

spark = SparkSession.builder.appName("agify-example").getOrCreate()
spark.dataSource.register(AgifyDataSource)
```

### Step 2: Read Age Prediction (Option: path = name, default "Michael")

```python
df = spark.read.format("agify").option("path", "Emma").load()
df.select("name", "age", "count").show()
```

### Step 3: Different Names

```python
df_michael = spark.read.format("agify").option("path", "Michael").load()
df_sarah = spark.read.format("agify").option("path", "Sarah").load()
```

### Example Output

```
+-----+---+-----+
|name |age|count|
+-----+---+-----+
|Emma |31 |12543|
+-----+---+-----+
```

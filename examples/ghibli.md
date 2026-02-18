# Ghibli API Data Source Example

Read Studio Ghibli film catalog from ghibliapi.vercel.app. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import GhibliDataSource

spark = SparkSession.builder.appName("ghibli-example").getOrCreate()
spark.dataSource.register(GhibliDataSource)
```

### Step 2: Read Films

```python
df = spark.read.format("ghibli").load()
df.select("title", "director", "release_date", "rt_score").show(10, truncate=False)
```

### Example Output

```
+---------------------------+-------------------+------------+--------+
|title                      |director           |release_date|rt_score|
+---------------------------+-------------------+------------+--------+
|Castle in the Sky          |Hayao Miyazaki     |1986        |95      |
|Grave of the Fireflies     |Isao Takahata      |1988        |97      |
+---------------------------+-------------------+------------+--------+
```

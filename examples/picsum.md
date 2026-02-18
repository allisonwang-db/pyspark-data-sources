# Lorem Picsum API Data Source Example

Read placeholder photo metadata from picsum.photos. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import PicsumDataSource

spark = SparkSession.builder.appName("picsum-example").getOrCreate()
spark.dataSource.register(PicsumDataSource)
```

### Step 2: Read Photo Metadata (Option: limit, default 30, max 100)

```python
df = spark.read.format("picsum").load()
df.select("id", "author", "width", "height", "url").show(10, truncate=40)
```

### Example Output

```
+---+------------------+-----+------+------------------------------------------+
|id |author            |width|height|url                                       |
+---+------------------+-----+------+------------------------------------------+
|0  |Alejandro Escamilla|5616 |3744  |https://unsplash.com/photos/...            |
|1  |Alejandro Escamilla|5616 |3744  |https://unsplash.com/photos/...            |
+---+------------------+-----+------+------------------------------------------+
```

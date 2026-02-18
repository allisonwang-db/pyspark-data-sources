# Met Museum API Data Source Example

Read artwork metadata from Metropolitan Museum of Art API. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import MetMuseumDataSource

spark = SparkSession.builder.appName("metmuseum-example").getOrCreate()
spark.dataSource.register(MetMuseumDataSource)
```

### Step 2: Read Artwork (Option: limit, default 50, max 100)

```python
df = spark.read.format("metmuseum").option("limit", "20").load()
df.select("objectID", "title", "artistDisplayName", "department").show(5, truncate=40)
```

### Example Output

```
+--------+------------------------------------------+------------------+---------------+
|objectID|title                                     |artistDisplayName |department     |
+--------+------------------------------------------+------------------+---------------+
|436535  |Design for a Ceiling Decoration           |Giovanni Domenico |European Paintings|
|436536  |Design for a Wall or Ceiling Decoration   |Giovanni Domenico |European Paintings|
+--------+------------------------------------------+------------------+---------------+
```

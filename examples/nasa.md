# NASA API Data Source Example

Read Astronomy Picture of the Day (APOD) and other NASA data. Requires a free API key from https://api.nasa.gov/.

## Setup Credentials

1. Get a free API key at https://api.nasa.gov/
2. Pass via option: `option("api_key", "YOUR_KEY")`

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import NasaDataSource

spark = SparkSession.builder.appName("nasa-example").getOrCreate()
spark.dataSource.register(NasaDataSource)
```

### Step 2: Read APOD (Default 5 entries)

```python
df = spark.read.format("nasa").option("api_key", "YOUR_API_KEY").load()
df.select("date", "title", "url").show(5, truncate=40)
```

### Step 3: Read More Entries

```python
df = spark.read.format("nasa").option("api_key", "YOUR_API_KEY").option("count", "20").load()
df.count()
```

### Example Output

```
+----------+----------------------------------------+------------------------+
|date      |title                                   |url                     |
+----------+----------------------------------------+------------------------+
|2012-01-21|Days in the Sun                         |https://apod.nasa.gov...|
|2014-10-07|From the Temple of the Sun to the Moon  |https://apod.nasa.gov...|
+----------+----------------------------------------+------------------------+
```

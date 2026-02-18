# RSS Data Source Example

Read RSS/Atom feed items. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import RssDataSource

spark = SparkSession.builder.appName("rss-example").getOrCreate()
spark.dataSource.register(RssDataSource)
```

### Step 2: Read Hacker News RSS (Default)

```python
df = spark.read.format("rss").load()
df.select("title", "link", "pub_date").show(10, truncate=40)
```

### Step 3: Read Custom Feed

```python
df = spark.read.format("rss").load("https://hnrss.org/frontpage")
df.count()
```

### Step 4: Filter by Keyword

```python
df = spark.read.format("rss").load()
df.filter("lower(title) LIKE '%python%'").select("title", "link").show(5, truncate=50)
```

# Reddit Data Source Example

Read posts from Reddit subreddits. No credentials required for public subreddits.

## Setup Credentials

None required for public subreddits.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import RedditDataSource

spark = SparkSession.builder.appName("reddit-example").getOrCreate()
spark.dataSource.register(RedditDataSource)
```

### Step 2: Read from r/python

```python
df = spark.read.format("reddit").load("python")
df.select("title", "author", "score", "num_comments").show(10, truncate=40)
```

### Step 3: Limit Results

```python
df = spark.read.format("reddit").option("limit", "50").load("programming")
df.count()
```

### Step 4: Top Posts by Score

```python
df = spark.read.format("reddit").load("python")
df.orderBy("score", ascending=False).select("title", "score", "num_comments").show(5, truncate=50)
```

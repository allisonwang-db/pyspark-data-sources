# Docker Hub Data Source Example

Read Docker image tags from Docker Hub. No credentials required for public images.

## Setup Credentials

None required for public repositories.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import DockerHubDataSource

spark = SparkSession.builder.appName("dockerhub-example").getOrCreate()
spark.dataSource.register(DockerHubDataSource)
```

### Step 2: Read Tags for Redis

```python
df = spark.read.format("dockerhub").load("library/redis")
df.select("name", "full_size", "last_updated").show(10, truncate=False)
```

### Step 3: Custom Page Size

```python
df = spark.read.format("dockerhub").option("page_size", "50").load("nginx")
df.count()
```

### Step 4: Largest Images

```python
df = spark.read.format("dockerhub").load("library/python")
df.orderBy("full_size", ascending=False).select("name", "full_size").show(5)
```

# Fake Data Source Example

Generate synthetic test data using Faker. Supports batch and streaming read. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import FakeDataSource

spark = SparkSession.builder.appName("fake-example").getOrCreate()
spark.dataSource.register(FakeDataSource)
```

### Step 2: Read Batch Data with Default Schema

```python
df = spark.read.format("fake").load()
df.show()
```

### Step 3: Custom Schema and Row Count

```python
# Field names must match Faker provider methods
df = (
    spark.read.format("fake")
    .schema("name string, email string, company string")
    .option("numRows", 5)
    .load()
)
df.show(truncate=False)
```

### Example Output

```
+------------------+-------------------------+-----------------+
|name              |email                    |company          |
+------------------+-------------------------+-----------------+
|Christine Sampson |johnsonjeremy@example.com|Hernandez-Nguyen |
|Yolanda Brown     |williamlowe@example.net  |Miller-Hernandez |
+------------------+-------------------------+-----------------+
```

## End-to-End Pipeline: Streaming Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import FakeDataSource

spark = SparkSession.builder.appName("fake-stream-example").getOrCreate()
spark.dataSource.register(FakeDataSource)
```

### Step 2: Stream Synthetic Data

```python
stream = (
    spark.readStream.format("fake")
    .option("rowsPerMicrobatch", 10)
    .load()
)

query = stream.writeStream.format("console").outputMode("append").trigger(once=True).start()
query.awaitTermination()
```

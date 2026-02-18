# Advice Slip API Data Source Example

Read random advice from api.adviceslip.com. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import AdviceDataSource

spark = SparkSession.builder.appName("advice-example").getOrCreate()
spark.dataSource.register(AdviceDataSource)
```

### Step 2: Read Advice (Option: limit, default 5, max 30)

```python
df = spark.read.format("advice").option("limit", "5").load()
df.show(truncate=False)
```

### Example Output

```
+---+------------------------------------------+
|id |advice                                    |
+---+------------------------------------------+
|1  |The best time to plant a tree was 20 years ago.|
|2  |Always take a chance on yourself.         |
+---+------------------------------------------+
```

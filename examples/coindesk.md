# CoinDesk API Data Source Example

Read current Bitcoin price (USD) from api.coindesk.com. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import CoinDeskDataSource

spark = SparkSession.builder.appName("coindesk-example").getOrCreate()
spark.dataSource.register(CoinDeskDataSource)
```

### Step 2: Read Bitcoin Price

```python
df = spark.read.format("coindesk").load()
df.select("code", "rate", "rate_float", "updated").show(truncate=False)
```

### Example Output

```
+----+--------+----------+---------------------------+
|code|rate   |rate_float|updated                    |
+----+--------+----------+---------------------------+
|USD |65,432.10|65432.1 |2025-02-18T12:00:00+00:00  |
+----+--------+----------+---------------------------+
```

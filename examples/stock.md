# Stock Data Source Example

Fetch stock market data from Alpha Vantage API. Batch read only.

## Setup Credentials

```bash
export ALPHA_VANTAGE_API_KEY="your-api-key"
```

Get a free API key at: https://www.alphavantage.co/support/#api-key

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
import os
from pyspark.sql import SparkSession
from pyspark_datasources import StockDataSource

spark = SparkSession.builder.appName("stock-example").getOrCreate()
spark.dataSource.register(StockDataSource)
```

### Step 2: Read Stock Data

```python
# Load single symbol
df = (
    spark.read.format("stock")
    .option("api_key", os.environ["ALPHA_VANTAGE_API_KEY"])
    .load("SPY")
)
df.show(5)
```

### Step 3: Optional - Multiple Symbols and Time Series

```python
# Multiple symbols (comma-separated)
df = (
    spark.read.format("stock")
    .option("api_key", os.environ["ALPHA_VANTAGE_API_KEY"])
    .option("function", "TIME_SERIES_DAILY")  # or TIME_SERIES_WEEKLY, TIME_SERIES_MONTHLY
    .load("AAPL, GOOGL, MSFT")
)
df.show()
```

### Example Output

```
+----------+------+------+------+------+--------+------+
|      date|  open|  high|   low| close|  volume|symbol|
+----------+------+------+------+------+--------+------+
|2024-06-04|526.46|529.15|524.96|528.39|33898396|   SPY|
|2024-06-03|529.02|529.31| 522.6| 527.8|46835702|   SPY|
+----------+------+------+------+------+--------+------+
```

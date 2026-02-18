# Robinhood Crypto Data Source Example

Read cryptocurrency market data from Robinhood Crypto API. Requires API credentials.

## Setup Credentials

```bash
export ROBINHOOD_API_KEY="your-api-key"
export ROBINHOOD_PRIVATE_KEY="base64-encoded-private-key"
```

Get credentials from: https://docs.robinhood.com/crypto/trading/
The private key must be base64-encoded.

```bash
pip install pyspark-data-sources[robinhood]
```

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
import os
from pyspark.sql import SparkSession
from pyspark_datasources import RobinhoodDataSource

spark = SparkSession.builder.appName("robinhood-example").getOrCreate()
spark.dataSource.register(RobinhoodDataSource)
```

### Step 2: Read Crypto Market Data

```python
df = (
    spark.read.format("robinhood")
    .option("api_key", os.environ["ROBINHOOD_API_KEY"])
    .option("private_key", os.environ["ROBINHOOD_PRIVATE_KEY"])
    .load("ETH-USD, BTC-USD")  # Comma-separated symbols
)
df.show()
```

### Example Output

```
+------+--------+------+-----+-----+------+
|symbol|  price |  ... | ... | ... | ...  |
+------+--------+------+-----+-----+------+
|ETH-USD| 3500.2|  ... | ... | ... | ...  |
|BTC-USD| 97000 |  ... | ... | ... | ...  |
+------+--------+------+-----+-----+------+
```

# Salesforce Data Source Example

Write streaming data to Salesforce objects (Account, Contact, Opportunity, etc.). Write-only, streaming.

## Setup Credentials

```bash
export SALESFORCE_USERNAME="your-username@company.com"
export SALESFORCE_PASSWORD="your-password"
export SALESFORCE_SECURITY_TOKEN="your-security-token"
```

Get security token from: Salesforce → Settings → My Personal Information → Reset My Security Token

```bash
pip install pyspark-data-sources[salesforce]
```

## End-to-End Pipeline: Streaming Write

### Step 1: Create Spark Session and Register Data Source

```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark_datasources import SalesforceDataSource

spark = SparkSession.builder.appName("salesforce-example").getOrCreate()
spark.dataSource.register(SalesforceDataSource)
```

### Step 2: Create Streaming Source and Transform to Salesforce Schema

```python
# Use rate source to simulate incoming data
streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 2).load()

# Transform to Account object schema
account_data = streaming_df.select(
    col("value").cast("string").alias("Name"),
    lit("Technology").alias("Industry"),
    (col("value") * 100000).cast("double").alias("AnnualRevenue"),
)
```

### Step 3: Write Stream to Salesforce

```python
query = (
    account_data.writeStream.format("pyspark.datasource.salesforce")
    .option("username", os.environ["SALESFORCE_USERNAME"])
    .option("password", os.environ["SALESFORCE_PASSWORD"])
    .option("security_token", os.environ["SALESFORCE_SECURITY_TOKEN"])
    .option("salesforce_object", "Account")
    .option("batch_size", "100")
    .option("checkpointLocation", "/tmp/salesforce_checkpoint")
    .trigger(processingTime="10 seconds")
    .start()
)
# query.awaitTermination()
```

### Optional: Write to Contact Object

```python
contact_data = streaming_df.select(
    col("value").cast("string").alias("FirstName"),
    lit("Doe").alias("LastName"),
    lit("contact@example.com").alias("Email"),
)
(
    contact_data.writeStream.format("pyspark.datasource.salesforce")
    .option("username", os.environ["SALESFORCE_USERNAME"])
    .option("password", os.environ["SALESFORCE_PASSWORD"])
    .option("security_token", os.environ["SALESFORCE_SECURITY_TOKEN"])
    .option("salesforce_object", "Contact")
    .option("checkpointLocation", "/tmp/salesforce_contact_checkpoint")
    .start()
)
```

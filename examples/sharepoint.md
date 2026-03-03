## Sharepoint Data Source Example

Write (streaming) data to Sharepoint Lists. Write-only, streaming.

## Setup / Retrieve Credentials
This datasource requires client-credentials (client-id, client-secret) using a Service-Principal
for authentication. The following is required and can be retrieved from Microsoft-Entra & your Sharepoint instance:
- Client-ID
- Client-Secret
- Tenant-ID (your Microsoft Tenant)
- Sharepoint Site-ID (the target Sharepoint Site hosting the target Sharepoint List)
- Sharepoint List-ID (the target Sharepoint List)

Use these credentials to configure the datasource as depicted below.

```bash
pip install pyspark-data-sources[sharepoint]
```

## End-to-End Pipeline: Streaming Write

### Step 1: Create Spark Session and Register Data Source

```python
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark_datasources import SharepointDataSource

spark = SparkSession.builder.appName("sharepoint-example").getOrCreate()
spark.dataSource.register(SharepointDataSource)
```

### Step 2: Create Streaming Source and Transform to Sharepoint Schema

```python
# Use rate source to simulate incoming data
df = (
        spark.readStream.format("rate").option("rowsPerSecond", 10).load()
        .select(
            col("value").cast("string").alias("name"),   
            lit("Technology").alias("industry"),
            (col("value") * 100000).cast("double").alias("annual_revenue")
        )
)
```

### Step 3: Write Stream to Sharepoint

```python
query = (
    df.writeStream
      .format("sharepoint")
      .option("sharepoint.auth.tenant_id", "<replace>")
      .option("sharepoint.auth.client_id", "<replace>")
      .option("sharepoint.auth.client_secret", "<replace>")
      .option("sharepoint.resource", "list")
      .option("sharepoint.site_id", "<replace>")
      .option("sharepoint.list.list_id", "<replace>")
      .option("sharepoint.list.fields", json.dumps({"name": "Name", "industry": "Industry", "annual_revenue": "AnnualRevenue"}))
      .option("sharepoint.batch_size", "200")
      .option("sharepoint.fail_fast", "true")
      .option("checkpointLocation", "/Volumes/test/chk/sharepoint")
      .start()
)
```

## Features
- **Write-only datasource**: Designed specifically for writing data to Sharepoint
- **Stream processing**: Uses Microsoft Graph API for efficient concurrent writes based on configurable batch-size parameter
- **Exactly-once semantics**: Integrates with Spark's checkpoint mechanism
- **Error handling**: Control over whether to fail the write operation if a record fails to be written
- **Flexible resource implementations**: Supports multiple resource types (currently only `list`)
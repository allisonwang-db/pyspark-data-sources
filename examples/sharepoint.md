# SharePoint Data Source Example

Write streaming data to SharePoint lists using the Microsoft Graph API. Write-only, streaming.

## Setup Credentials

```python
SHAREPOINT_CLIENT_ID = "your-azure-app-client-id"
SHAREPOINT_CLIENT_SECRET = "your-azure-app-client-secret"
SHAREPOINT_TENANT_ID = "your-azure-tenant-id"
SHAREPOINT_SITE_ID = "your-sharepoint-site-id"  # From site URL
SHAREPOINT_LIST_ID = "your-sharepoint-list-id"  # GUID of the list
```

Register an Azure AD app and grant it SharePoint/Microsoft Graph permissions. Get the site ID and list ID from your SharePoint admin center or Graph API.

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

### Step 2: Create Streaming Source and Transform to SharePoint Schema

```python
# Use rate source to simulate incoming data
streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()

# Transform to match your SharePoint list columns
list_data = streaming_df.select(
    col("value").cast("string").alias("name"),
    lit("Technology").alias("industry"),
    (col("value") * 100000).cast("double").alias("annual_revenue"),
)
```

### Step 3: Write Stream to SharePoint

```python
# Map Spark column names to SharePoint list column names
field_mapping = json.dumps({
    "name": "Name",
    "industry": "Industry",
    "annual_revenue": "AnnualRevenue",
})

query = (
    list_data.writeStream
    .format("pyspark.datasource.sharepoint")
    .option("pyspark.datasource.sharepoint.auth.client_id", SHAREPOINT_CLIENT_ID)
    .option("pyspark.datasource.sharepoint.auth.client_secret", SHAREPOINT_CLIENT_SECRET)
    .option("pyspark.datasource.sharepoint.auth.tenant_id", SHAREPOINT_TENANT_ID)
    .option("pyspark.datasource.sharepoint.resource", "list")
    .option("pyspark.datasource.sharepoint.site_id", SHAREPOINT_SITE_ID)
    .option("pyspark.datasource.sharepoint.list.list_id", SHAREPOINT_LIST_ID)
    .option("pyspark.datasource.sharepoint.list.fields", field_mapping)
    .option("pyspark.datasource.sharepoint.batch_size", "200")
    .option("pyspark.datasource.sharepoint.fail_fast", "true")
    .option("checkpointLocation", "/path/to/checkpoint")
    .start()
)
# query.awaitTermination()
```

### Key Options

| Option | Description |
|--------|-------------|
| `auth.client_id` | Azure AD app client ID |
| `auth.client_secret` | Azure AD app client secret |
| `auth.tenant_id` | Azure AD tenant ID |
| `resource` | Resource type (currently `list`) |
| `site_id` | SharePoint site ID |
| `list.list_id` | SharePoint list GUID |
| `list.fields` | JSON map of Spark column → SharePoint column |
| `batch_size` | Records per batch (default: 200) |
| `fail_fast` | Fail on first error (default: false) |

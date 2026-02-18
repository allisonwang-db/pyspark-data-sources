# Notion Data Source

Read and write data to Notion databases using PySpark.

## Installation

```bash
pip install pyspark-data-sources[notion]
```

## Usage

### Reading from Notion

Read pages from a Notion database.

```python
from pyspark.sql import SparkSession
from pyspark_datasources import NotionDataSource

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(NotionDataSource)

# Read from a Notion database
df = spark.read.format("notion") \
    .option("token", "secret_...") \
    .option("database_id", "your_database_id") \
    .load()

df.show()
# +--------------------+-----------+--------------------+--------------------+--------------------+--------------------+
# |                  id|      title|        created_time|    last_edited_time|                 url|          properties|
# +--------------------+-----------+--------------------+--------------------+--------------------+--------------------+
# |12345678-abcd-123...|Test Page 1|2023-01-01T00:00:...|2023-01-02T00:00:...|https://notion.so...|{"Name": {"id": "...|
# +--------------------+-----------+--------------------+--------------------+--------------------+--------------------+
```

### Writing to Notion

Write data to a Notion database. The input DataFrame should have a `properties` column containing a JSON string of the properties to set for each new page.

```python
from pyspark.sql import SparkSession, Row
import json

spark = SparkSession.builder.getOrCreate()
spark.dataSource.register(NotionDataSource)

# Create data to write
data = [
    Row(properties=json.dumps({
        "Name": {
            "title": [{"text": {"content": "New Page from Spark"}}]
        },
        "Status": {
            "select": {"name": "In Progress"}
        }
    }))
]

df = spark.createDataFrame(data)

# Write to Notion
df.write.format("notion") \
    .mode("append") \
    .option("token", "secret_...") \
    .option("database_id", "your_database_id") \
    .save()
```

## Schema

The data source uses a fixed schema:

```
id string,
title string,
created_time timestamp,
last_edited_time timestamp,
url string,
properties string
```

- `id`: The page ID.
- `title`: The title of the page (extracted from the `title` property).
- `created_time`: The creation timestamp.
- `last_edited_time`: The last edited timestamp.
- `url`: The URL of the page.
- `properties`: A JSON string containing all page properties.

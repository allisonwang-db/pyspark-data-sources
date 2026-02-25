# Google Sheets Data Source Example

Read data from public Google Sheets. Sheet must be shared with "Anyone with the link can view."

## Setup Credentials

None required for **public sheets**. Ensure the sheet is shared: Share → "Anyone with the link" → Viewer.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import GoogleSheetsDataSource

spark = SparkSession.builder.appName("googlesheets-example").getOrCreate()
spark.dataSource.register(GoogleSheetsDataSource)
```

### Step 2: Read from Google Sheet URL

```python
# Use a public Google Sheet URL
sheet_url = "https://docs.google.com/spreadsheets/d/1H7bKPGpAXbPRhTYFxqg1h6FmKl5ZhCTM_5OlAqCHfVs"
df = spark.read.format("googlesheets").load(sheet_url)
df.show()
```

### Step 3: Optional - Read Specific Sheet by Name or Index

```python
df = (
    spark.read.format("googlesheets")
    .option("sheetName", "Sheet2")
    .load(sheet_url)
)

# Or by 0-based index
df = (
    spark.read.format("googlesheets")
    .option("sheetIndex", "1")
    .load(sheet_url)
)
```

### Example Output

```
+------+----+-------+
|Name  |Age |City   |
+------+----+-------+
|Alice |25  |NYC    |
|Bob   |30  |LA     |
|Carol |28  |Chicago|
+------+----+-------+
```

# Excel Data Source Example

Read Excel files (.xlsx). Requires: `pip install pyspark-data-sources[excel]`

## Setup Credentials

```bash
pip install pyspark-data-sources[excel]
```

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import ExcelDataSource

spark = SparkSession.builder.appName("excel-example").getOrCreate()
spark.dataSource.register(ExcelDataSource)
```

### Step 2: Read Excel File

```python
df = spark.read.format("excel").load("/path/to/file.xlsx")
df.show()
```

### Step 3: Read Specific Sheet

```python
df = spark.read.format("excel").option("sheet", "1").load("/path/to/file.xlsx")
df.show()
```

### Step 4: Without Header Row

```python
df = spark.read.format("excel").option("header", "false").load("/path/to/file.xlsx")
df.show()
```

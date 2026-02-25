# Arrow Data Source Example

Read Apache Arrow files (.arrow) into Spark DataFrames. No credentials required.

## Setup Credentials

None required. Arrow reads from local paths or cloud storage.

## End-to-End Pipeline: Read Arrow Files

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import ArrowDataSource

spark = SparkSession.builder.appName("arrow-example").getOrCreate()
spark.dataSource.register(ArrowDataSource)
```

### Step 2: Write Sample Data to Arrow (for this example)

First create a sample Arrow file to read:

```python
import pyarrow as pa

# Create sample data
table = pa.table({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [28, 34, 22]
})

# Write to Arrow file
output_path = "/tmp/sample_data.arrow"
with pa.ipc.new_file(output_path, table.schema) as writer:
    writer.write_table(table)
```

### Step 3: Read Arrow File with Data Source

```python
df = spark.read.format("arrow").load(output_path)
df.show()
```

### Example Output

```
+---+-------+---+
| id|   name|age|
+---+-------+---+
|  1|  Alice| 28|
|  2|    Bob| 34|
|  3|Charlie| 22|
+---+-------+---+
```

### Optional: Read Multiple Files or Directory

```python
# Read all .arrow files in a directory
df = spark.read.format("arrow").load("/path/to/arrow/files/")

# Read with glob pattern
df = spark.read.format("arrow").load("/path/to/sales_*.arrow")
```

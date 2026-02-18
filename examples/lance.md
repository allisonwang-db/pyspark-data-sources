# Lance Data Source Example

Write Spark DataFrames to Lance format (vector database format). No credentials required.

## Setup Credentials

None required. Writes to local path or cloud storage.

```bash
pip install pyspark-data-sources[lance]
```

## End-to-End Pipeline: Batch Write

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import LanceSink

spark = SparkSession.builder.appName("lance-example").getOrCreate()
spark.dataSource.register(LanceSink)
```

### Step 2: Create Sample Data

```python
# Create DataFrame with optional vector column
df = spark.createDataFrame(
    [(1, [0.1, 0.2, 0.3], 0.95), (2, [0.4, 0.5, 0.6], 0.88), (3, [0.7, 0.8, 0.9], 0.92)],
    schema="id long, vector array<double>, score double"
)
```

### Step 3: Write to Lance Format

```python
output_path = "/tmp/lance_dataset"
df.write.format("lance").mode("append").save(output_path)
```

### Step 4: Verify with Lance API

```python
import lance
ds = lance.LanceDataset(output_path)
ds.to_table().to_pandas()
```

### Example Output

```
   id              vector  score
0   1  [0.1, 0.2, 0.3]   0.95
1   2  [0.4, 0.5, 0.6]   0.88
2   3  [0.7, 0.8, 0.9]   0.92
```

# Hugging Face Data Source Example

Load datasets from the Hugging Face Hub into Spark. Public datasets only; no credentials required.

## Setup Credentials

None required for public datasets. Ensure the `datasets` library is installed:

```bash
pip install pyspark-data-sources[datasets]
```

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import HuggingFaceDatasets

spark = SparkSession.builder.appName("huggingface-example").getOrCreate()
spark.dataSource.register(HuggingFaceDatasets)
```

### Step 2: Read Dataset from Hugging Face Hub

```python
df = spark.read.format("huggingface").load("imdb")
df.select("text", "label").show(3, truncate=50)
```

### Step 3: Optional - Read Specific Split

```python
df = (
    spark.read.format("huggingface")
    .option("split", "test")
    .load("imdb")
)
```

### Example Output

```
+--------------------------------------------------+-----+
|text                                              |label|
+--------------------------------------------------+-----+
|I rented this movie last night and I must say... |0    |
|This film is absolutely fantastic! The acting... |1    |
+--------------------------------------------------+-----+
```

# Kaggle Data Source Example

Load datasets from Kaggle. Public datasets work without credentials; private datasets require API keys.

## Setup Credentials

For **public datasets**: Optional. Kaggle may use cached credentials from `~/.kaggle/kaggle.json`.

For **private datasets**: Create `~/.kaggle/kaggle.json`:

```json
{"username":"your-kaggle-username","key":"your-kaggle-api-key"}
```

Get API keys at: https://www.kaggle.com/settings

```bash
pip install pyspark-data-sources[kaggle]
```

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import KaggleDataSource

spark = SparkSession.builder.appName("kaggle-example").getOrCreate()
spark.dataSource.register(KaggleDataSource)
```

### Step 2: Read Public Dataset

```python
# Format: owner/dataset for handle, then file path in load()
df = (
    spark.read.format("kaggle")
    .options(handle="yasserh/titanic-dataset")
    .load("Titanic-Dataset.csv")
)
df.select("PassengerId", "Name", "Age", "Survived").show(5)
```

### Step 3: Optional - Read with Explicit Credentials

```python
df = (
    spark.read.format("kaggle")
    .options(
        handle="owner/private-dataset",
        username="your-kaggle-username",
        key="your-kaggle-api-key",
    )
    .load("data.csv")
)
```

### Example Output

```
+-----------+--------------------+----+--------+
|PassengerId|Name                |Age |Survived|
+-----------+--------------------+----+--------+
|1          |Braund, Mr. Owen    |22.0|0       |
|2          |Cumings, Mrs. John  |38.0|1       |
+-----------+--------------------+----+--------+
```

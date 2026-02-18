# ReqRes API Data Source Example

Read test users from reqres.in. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import ReqresDataSource

spark = SparkSession.builder.appName("reqres-example").getOrCreate()
spark.dataSource.register(ReqresDataSource)
```

### Step 2: Read Users (Option: page, default 1)

```python
df = spark.read.format("reqres").load()
df.select("id", "email", "first_name", "last_name").show()
```

### Step 3: Read Specific Page

```python
df_page2 = spark.read.format("reqres").option("page", "2").load()
df_page2.show()
```

### Example Output

```
+---+----------------------+----------+---------+
|id |email                 |first_name|last_name|
+---+----------------------+----------+---------+
|1  |george.bluth@reqres.in|George    |Bluth    |
|2  |janet.weaver@reqres.in|Janet     |Weaver   |
+---+----------------------+----------+---------+
```

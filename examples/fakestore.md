# Fake Store API Data Source Example

Read e-commerce test data from fakestoreapi.com. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import FakeStoreDataSource

spark = SparkSession.builder.appName("fakestore-example").getOrCreate()
spark.dataSource.register(FakeStoreDataSource)
```

### Step 2: Read Products (Default Endpoint)

```python
df = spark.read.format("fakestore").load()
df.select("id", "title", "price", "category", "rating").show(10, truncate=40)
```

### Step 3: Read Other Endpoints (carts, users, etc.)

```python
users_df = spark.read.format("fakestore").option("endpoint", "users").load()
users_df.show(5)
```

### Example Output

```
+---+------------------------------------------+-----+----------+------+
|id |title                                     |price|category  |rating|
+---+------------------------------------------+-----+----------+------+
|1  |Fjallraven - Foldsack No. 1 Backpack...   |109.95|men's clothing|3.9 |
|2  |Mens Casual Premium Slim Fit T-Shirts...  |22.3 |men's clothing|4.1 |
+---+------------------------------------------+-----+----------+------+
```

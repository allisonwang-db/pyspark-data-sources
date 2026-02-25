# JSONPlaceholder Data Source Example

Read test data from the free JSONPlaceholder API. No credentials required. Useful for prototyping and joins.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import JSONPlaceholderDataSource

spark = SparkSession.builder.appName("jsonplaceholder-example").getOrCreate()
spark.dataSource.register(JSONPlaceholderDataSource)
```

### Step 2: Read Posts (Default Endpoint)

```python
df = spark.read.format("jsonplaceholder").load()
df.select("userId", "id", "title").show(5, truncate=False)
```

### Step 3: Read Other Endpoints

```python
# Available: posts, users, todos, comments, albums, photos
users_df = spark.read.format("jsonplaceholder").option("endpoint", "users").load()
posts_df = spark.read.format("jsonplaceholder").option("endpoint", "posts").load()
```

### Step 4: Join Related Data (Referential Integrity)

```python
posts_with_authors = posts_df.join(users_df, posts_df.userId == users_df.id)
posts_with_authors.select("title", "name", "email").show(3, truncate=False)
```

### Example Output

```
+-----------------------------------------------+------------------+------------------------+
|title                                          |name              |email                   |
+-----------------------------------------------+------------------+------------------------+
|sunt aut facere repellat provident...          |Leanne Graham     |Sincere@april.biz       |
|qui est esse                                   |Leanne Graham     |Sincere@april.biz       |
+-----------------------------------------------+------------------+------------------------+
```

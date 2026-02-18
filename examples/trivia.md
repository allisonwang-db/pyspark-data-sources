# Open Trivia Database Data Source Example

Read trivia questions from opentdb.com. No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import TriviaDataSource

spark = SparkSession.builder.appName("trivia-example").getOrCreate()
spark.dataSource.register(TriviaDataSource)
```

### Step 2: Read Trivia Questions (Option: amount, default 10, max 50)

```python
df = spark.read.format("trivia").option("amount", "15").load()
df.select("category", "difficulty", "question", "correct_answer").show(5, truncate=40)
```

### Example Output

```
+------------------+----------+----------------------------------------+---------------+
|category          |difficulty|question                                |correct_answer |
+------------------+----------+----------------------------------------+---------------+
|Science: Computers|easy      |What does CPU stand for?                 |Central Proces...|
|Entertainment: Film|medium   |Which film has the most Academy Awards? |Ben-Hur         |
+------------------+----------+----------------------------------------+---------------+
```

# Jira Data Source Example

Read and write Jira issues. Requires Jira credentials.

## Setup Credentials

```python
JIRA_URL = "https://your-domain.atlassian.net"
JIRA_USERNAME = "your-email@example.com"
JIRA_TOKEN = "your-api-token"
```

Create an API token at: https://id.atlassian.com/manage-profile/security/api-tokens

Install the Jira client: `pip install pyspark-data-sources[jira]`

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import JiraDataSource

spark = SparkSession.builder.appName("jira-read-example").getOrCreate()
spark.dataSource.register(JiraDataSource)
```

### Step 2: Read Issues with JQL

```python
df = (
    spark.read.format("jira")
    .option("url", JIRA_URL)
    .option("username", JIRA_USERNAME)
    .option("token", JIRA_TOKEN)
    .option("jql", "project = PROJ ORDER BY created DESC")
    .load()
)
df.select("key", "summary", "status", "assignee").show()
```

### Example Output

```
+------+------------------+--------+---------+
|key   |summary           |status  |assignee |
+------+------------------+--------+---------+
|PROJ-1|Fix login bug     |Open    |John Doe |
|PROJ-2|Update docs       |In Prog |Jane Smith|
+------+------------------+--------+---------+
```

## End-to-End Pipeline: Batch Write (Create)

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession, Row
from pyspark_datasources import JiraDataSource

spark = SparkSession.builder.appName("jira-write-example").getOrCreate()
spark.dataSource.register(JiraDataSource)
```

### Step 2: Create Sample Data for New Issues

```python
new_issues = [
    Row(summary="New task from Spark", description="Created via PySpark", priority="Medium"),
    Row(summary="Bug report", description="Issue found in module X", priority="High"),
]
df = spark.createDataFrame(new_issues)
```

### Step 3: Write to Jira

```python
(
    df.write.format("jira")
    .option("url", JIRA_URL)
    .option("username", JIRA_USERNAME)
    .option("token", JIRA_TOKEN)
    .option("project", "PROJ")
    .option("issuetype", "Task")
    .mode("append")
    .save()
)
```

### Step 4: Optional - Update Existing Issues

Include the `key` column to update instead of create:

```python
updates = [Row(key="PROJ-123", summary="Updated title", description="New description")]
df_updates = spark.createDataFrame(updates)
(
    df_updates.write.format("jira")
    .option("url", JIRA_URL)
    .option("username", JIRA_USERNAME)
    .option("token", JIRA_TOKEN)
    .mode("append")
    .save()
)
```

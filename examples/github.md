# GitHub Data Source Example

Read pull request data from public GitHub repositories. Optional token for private repos and higher rate limits.

## Setup Credentials

For **public repositories**: No credentials required (subject to 60 req/hour rate limit).

For **private repos** or **higher rate limits**: Set a GitHub Personal Access Token:

```python
GITHUB_TOKEN = "ghp_your_personal_access_token"
```

Create a token at: https://github.com/settings/tokens (scope: `repo` for private repos).

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import GithubDataSource

spark = SparkSession.builder.appName("github-example").getOrCreate()
spark.dataSource.register(GithubDataSource)
```

### Step 2: Read Pull Requests from a Public Repository

```python
# No token needed for public repos
df = spark.read.format("github").load("apache/spark")
df.select("id", "title", "author", "created_at").show(5, truncate=False)
```

### Step 3: Optional - Use Token for Private Repo or Rate Limits

```python
df = (
    spark.read.format("github")
    .option("token", GITHUB_TOKEN)
    .load("owner/private-repo")
)
df.show()
```

### Example Output

```
+---+--------------------+---------------+--------------------+
| id|               title|         author|          created_at|
+---+--------------------+---------------+--------------------+
|  1|Initial commit      |          matei|2014-02-03T18:47:...|
|  2|Revert "Initial ... |          matei|2014-02-03T18:47:...|
+---+--------------------+---------------+--------------------+
```

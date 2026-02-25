# Jira Data Source Design

## Overview

The Jira Data Source enables Apache Spark to read from and write to Jira instances using the Python Data Source API. It allows users to query Jira issues using JQL (Jira Query Language) and create or update issues directly from Spark DataFrames.

## Architecture

The connector uses the `jira` Python library to interact with the Jira REST API.

### Dependencies

-   `jira`: Python library for Jira REST API interaction.
-   `pyspark`: Apache Spark Python API.

## Schema

The data source uses a fixed schema representing common Jira issue fields:

```sql
key string,
summary string,
status string,
description string,
created string,
updated string,
assignee string,
reporter string,
priority string,
issuetype string,
project string
```

## Read Path

The read path is implemented in `JiraDataSourceReader`.

1.  **Configuration**: Accepts `url`, `username`, `token`, and `jql` options.
2.  **Execution**:
    -   Initializes a `JIRA` client using basic authentication.
    -   Executes the JQL query using `jira.search_issues`.
    -   Handles pagination automatically (fetching 50 issues per batch).
    -   Maps Jira Issue objects to Spark Rows based on the schema.

### Example (Read)

```python
df = spark.read.format("jira") \
    .option("url", "https://your-domain.atlassian.net") \
    .option("username", "user@example.com") \
    .option("token", "api-token") \
    .option("jql", "project = PROJ AND status = 'Open'") \
    .load()
```

## Write Path

The write path is implemented in `JiraDataSourceWriter`. It supports both **Creating** new issues and **Updating** existing ones.

1.  **Configuration**: Accepts `url`, `username`, `token`, `project` (optional for updates), and `issuetype` (optional).
2.  **Execution**:
    -   Iterates through the partition data.
    -   **Update Logic**: If the `key` column exists and is not null:
        -   It treats the row as an update to the issue with that key.
        -   Read-only fields (e.g., `created`, `updated`, `status`) are ignored.
        -   Updates the issue using `issue.update(fields=...)`.
    -   **Create Logic**: If the `key` column is missing or null:
        -   It treats the row as a new issue.
        -   Requires `project` and `issuetype` (either from options or columns).
        -   Creates the issue using `jira.create_issue(fields=...)`.

### Example (Write - Create New Issues)

To create new issues, do not provide a `key` column. You must specify the `project` and `issuetype` either in the DataFrame or as options.

```python
from pyspark.sql import Row

# Create a DataFrame with new issues
new_issues = [
    Row(summary="Fix login bug", description="Login fails with 500 error", priority="High"),
    Row(summary="Update documentation", description="Add Jira connector docs", priority="Medium")
]
df = spark.createDataFrame(new_issues)

# Write to Jira
df.write.format("jira") \
    .option("url", "https://your-domain.atlassian.net") \
    .option("username", "user@example.com") \
    .option("token", "api-token") \
    .option("project", "PROJ") \
    .option("issuetype", "Task") \
    .save()
```

### Example (Write - Update Existing Issues)

To update existing issues, include the `key` column in your DataFrame.

```python
from pyspark.sql import Row

# Create a DataFrame with updates
updates = [
    Row(key="PROJ-123", summary="Updated Summary", description="New description"),
    Row(key="PROJ-124", priority="Low")
]
df = spark.createDataFrame(updates)

# Write updates to Jira
df.write.format("jira") \
    .option("url", "https://your-domain.atlassian.net") \
    .option("username", "user@example.com") \
    .option("token", "api-token") \
    .save()
```

## Error Handling

-   **Connection Errors**: Raises exceptions if connection to Jira fails.
-   **Missing Dependencies**: Raises `ImportError` if `jira` library is not installed.
-   **Validation**: Validates required options (`url`, `username`, `token`).

## Future Improvements

-   Support for custom fields (dynamic schema).
-   Support for more complex authentication methods (OAuth).
-   Parallel reading (partitioning by date or ID range).

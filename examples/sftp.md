# SFTP Data Source Example

Read and write text files from SFTP servers. Supports batch read and batch write.

## Setup Credentials

**Option A: Password authentication**

```python
SFTP_HOST = "sftp.example.com"
SFTP_USERNAME = "your-username"
SFTP_PASSWORD = "your-password"
```

**Option B: SSH key authentication**

```python
SFTP_HOST = "sftp.example.com"
SFTP_USERNAME = "your-username"
SFTP_KEY_FILENAME = "/path/to/private_key"
```

```bash
pip install pyspark-data-sources[sftp]
```

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import SFTPDataSource

spark = SparkSession.builder.appName("sftp-read-example").getOrCreate()
spark.dataSource.register(SFTPDataSource)
```

### Step 2: Read File from SFTP

```python
df = (
    spark.read.format("sftp")
    .option("host", SFTP_HOST)
    .option("username", SFTP_USERNAME)
    .option("password", SFTP_PASSWORD)
    .option("path", "/remote/path/data.txt")
    .load()
)
df.show()
```

### Step 3: Optional - Read Directory or Use Key File

```python
df = (
    spark.read.format("sftp")
    .option("host", SFTP_HOST)
    .option("username", SFTP_USERNAME)
    .option("key_filename", SFTP_KEY_FILENAME)
    .option("path", "/remote/directory/")
    .option("recursive", "true")
    .load()
)
```

### Example Output

```
+--------------------+
|value               |
+--------------------+
|line 1 content      |
|line 2 content      |
+--------------------+
```

## End-to-End Pipeline: Batch Write

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import SFTPDataSource

spark = SparkSession.builder.appName("sftp-write-example").getOrCreate()
spark.dataSource.register(SFTPDataSource)
```

### Step 2: Create Sample Data

```python
data = [("line 1",), ("line 2",), ("line 3",)]
df = spark.createDataFrame(data, schema="value string")
```

### Step 3: Write to SFTP

```python
(
    df.write.format("sftp")
    .option("host", SFTP_HOST)
    .option("username", SFTP_USERNAME)
    .option("password", SFTP_PASSWORD)
    .option("path", "/remote/path/output.txt")
    .save()
)
```

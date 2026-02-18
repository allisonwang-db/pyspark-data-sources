# Oracle Data Source Example

Read and write data to Oracle databases using the `oracledb` library.

## Setup Credentials

```python
ORACLE_USER = "oracle_user"
ORACLE_PASSWORD = "your-password"
ORACLE_HOST = "oracle.example.com"  # or "localhost"
ORACLE_PORT = "1521"
ORACLE_SID = "ORCL"  # or use service_name instead
```

Install the Oracle driver: `pip install pyspark-data-sources[oracledb]`

## End-to-End Pipeline: Batch Read (Table)

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import OracleDataSource

spark = SparkSession.builder.appName("oracle-read-example").getOrCreate()
spark.dataSource.register(OracleDataSource)
```

### Step 2: Read Table

```python
df = (
    spark.read.format("oracle")
    .option("user", ORACLE_USER)
    .option("password", ORACLE_PASSWORD)
    .option("host", ORACLE_HOST)
    .option("port", ORACLE_PORT)
    .option("sid", ORACLE_SID)
    .option("dbtable", "EMPLOYEES")
    .load()
)
df.select("EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME", "DEPARTMENT_ID").show()
```

## End-to-End Pipeline: Batch Read (Query)

```python
df = (
    spark.read.format("oracle")
    .option("user", ORACLE_USER)
    .option("password", ORACLE_PASSWORD)
    .option("host", ORACLE_HOST)
    .option("query", "SELECT * FROM EMPLOYEES WHERE DEPARTMENT_ID = 10")
    .load()
)
df.show()
```

## End-to-End Pipeline: Batch Write

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession, Row
from pyspark_datasources import OracleDataSource

spark = SparkSession.builder.appName("oracle-write-example").getOrCreate()
spark.dataSource.register(OracleDataSource)
```

### Step 2: Create Sample Data

```python
data = [
    Row(id=1, name="Alice", dept_id=10),
    Row(id=2, name="Bob", dept_id=20),
]
df = spark.createDataFrame(data)
```

### Step 3: Write to Oracle

```python
(
    df.write.format("oracle")
    .option("user", ORACLE_USER)
    .option("password", ORACLE_PASSWORD)
    .option("host", ORACLE_HOST)
    .option("port", ORACLE_PORT)
    .option("sid", ORACLE_SID)
    .option("dbtable", "NEW_EMPLOYEES")
    .mode("append")
    .save()
)
```

### Options

| Option | Description | Required |
|--------|-------------|----------|
| `user` | Database username | Yes |
| `password` | Database password | Yes |
| `host` | Hostname (default: localhost) | No |
| `port` | Port (default: 1521) | No |
| `sid` | Oracle SID | No |
| `service_name` | Service name (alternative to SID) | No |
| `dsn` | Full DSN connection string | No |
| `dbtable` | Table name (read or write) | Yes for write / table read |
| `query` | SQL query (read only) | No |

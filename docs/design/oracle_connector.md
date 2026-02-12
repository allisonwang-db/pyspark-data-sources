# Oracle Data Source Design Document

## Overview

This document outlines the design for a new PySpark Data Source that enables reading and writing to Oracle databases. This connector will allow users to read data from Oracle tables or queries into a Spark DataFrame and write Spark DataFrames to Oracle tables.

## Motivation

Oracle Database is a widely used relational database management system. Enabling Spark to directly read from and write to Oracle databases simplifies ETL pipelines and data integration tasks.

## Design

### Dependencies

The connector will use the `oracledb` library (the new name for `cx_Oracle`) for Oracle database communication.

### Data Source Name

The data source will be registered as `oracle`.

### Options

The following options will be supported:

| Option | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `user` | The database username. | Yes | - |
| `password` | The database password. | Yes | - |
| `host` | The hostname or IP address of the Oracle server. | No | `localhost` |
| `port` | The port number of the Oracle server. | No | `1521` |
| `sid` | The System Identifier (SID) of the database. | No | - |
| `service_name` | The Service Name of the database. | No | - |
| `dsn` | The Data Source Name (connection string). If provided, overrides host/port/sid/service_name. | No | - |
| `dbtable` | The table name to read from or write to. | Yes (for write, or read table) | - |
| `query` | A SQL query to execute (Read only). | No | - |
| `partitionColumn` | The column name used for partitioning (Read only). | No | - |
| `lowerBound` | The minimum value of the partition column (Read only). | No | - |
| `upperBound` | The maximum value of the partition column (Read only). | No | - |
| `numPartitions` | The number of partitions (Read only). | No | - |

**Note**:
- Either `dbtable` or `query` must be provided for reading.
- `dbtable` is required for writing.
- If `partitionColumn` is specified, `lowerBound`, `upperBound`, and `numPartitions` are also required.

### Schema

- **Read**: The schema will be inferred from the query result metadata (`cursor.description`) or table definition.
    - `NUMBER` -> `DoubleType` (or `LongType`/`DecimalType` depending on precision/scale if available)
    - `VARCHAR2`, `CHAR`, `CLOB` -> `StringType`
    - `DATE`, `TIMESTAMP` -> `TimestampType`
- **Write**: The input DataFrame schema will be used to create the table (if it doesn't exist) or map to existing columns.

### Read Path (Batch)

1.  **Connect**: Establish a connection using `oracledb`.
2.  **Schema Inference**: Execute a query with `LIMIT 0` or similar to get metadata and infer schema.
3.  **Partition**:
    -   If partitioning options are provided: Generate `WHERE` clauses for each partition range.
    -   If not: Create a single partition.
4.  **Read**: In each task:
    -   Connect to Oracle.
    -   Execute the query (with partition filter if applicable).
    -   Fetch rows and convert to Spark Row objects.

### Write Path (Batch)

1.  **Driver**: Validate options.
2.  **Executor**:
    -   Connect to Oracle.
    -   Prepare `INSERT` statement.
    -   Iterate over rows and execute batch inserts.
    -   Commit transaction.
3.  **Modes**:
    -   `Append`: Append data to existing table.
    -   `Overwrite`: Truncate/Drop table before writing (careful implementation needed).
    -   `ErrorIfExists`: Fail if table exists.
    -   `Ignore`: Do nothing if table exists.

## Implementation Plan

1.  Add `oracledb` to `pyproject.toml`.
2.  Implement `pyspark_datasources/oracle.py`.
    -   `OracleDataSource` class.
    -   `OracleDataSourceReader` class.
    -   `OracleDataSourceWriter` class.
3.  Add unit/integration tests in `tests/test_oracle.py`.
    -   Mock `oracledb` for unit tests.
4.  Verify functionality.

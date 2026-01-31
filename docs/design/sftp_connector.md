# SFTP Data Source Design Document

## Overview

This document outlines the design for a new PySpark Data Source that enables batch reading and writing to SFTP servers. This connector will allow users to read text files from an SFTP server into a Spark DataFrame and write Spark DataFrames as text files to an SFTP server.

## Motivation

SFTP (Secure File Transfer Protocol) is a widely used protocol for secure file transfer. Enabling Spark to directly read from and write to SFTP servers simplifies ETL pipelines that involve legacy systems or external partners who exchange data via SFTP.

## Design

### Dependencies

The connector will use the `paramiko` library for SFTP communication.

### Data Source Name

The data source will be registered as `sftp`.

### Options

The following options will be supported:

| Option | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `host` | The hostname or IP address of the SFTP server. | Yes | - |
| `port` | The port number of the SFTP server. | No | `22` |
| `username` | The username for authentication. | Yes | - |
| `password` | The password for authentication. | No | - |
| `key_filename` | Path to a private key file for authentication. | No | - |
| `path` | The directory or file path on the SFTP server. If a directory is specified, all files in the directory will be read. | Yes | - |
| `recursive` | Whether to recursively list files in subdirectories when `path` is a directory (Read only). | No | `false` |
| `file_format` | The format of files to read/write. Currently supports `text`. | No | `text` |

**Note**: Either `password` or `key_filename` must be provided.

### Schema

For the initial implementation, we will support a fixed schema for the `text` format:

- **Read**: `value string` (each row represents a line in the file).
- **Write**: The input DataFrame must have a single column named `value` of type `string`.

Future versions may support `binary` (file content as bytes) or `csv`/`json` parsing.

### Read Path (Batch)

1.  **Connect**: Establish an SFTP connection using `paramiko`.
2.  **List**: List files in the specified `path`. If `recursive` is true, traverse subdirectories.
3.  **Partition**: Create partitions based on the list of files.
    -   *Strategy*: One partition per file, or group small files. For simplicity v1: One partition per file.
4.  **Read**: In each task:
    -   Connect to SFTP.
    -   Open the assigned file.
    -   Read line by line.
    -   Yield rows.

### Write Path (Batch)

1.  **Driver**: Validate options.
2.  **Executor**:
    -   Connect to SFTP.
    -   Generate a unique filename (e.g., `part-{task_id}-{uuid}.txt`) in the target `path`.
    -   Open the file for writing.
    -   Iterate over rows and write them to the file (adding newlines).
3.  **Commit**: Standard Spark commit protocol (handled by `DataSourceWriter`).

## Implementation Plan

1.  Add `paramiko` to `pyproject.toml`.
2.  Implement `pyspark_datasources/sftp.py`.
    -   `SFTPDataSource` class.
    -   `SFTPDataSourceReader` class.
    -   `SFTPDataSourceWriter` class.
3.  Add unit/integration tests in `tests/test_sftp.py`.
    -   Mock SFTP server or use `mock` library to simulate `paramiko` behavior.
4.  Verify functionality.

## Testing

To perform an end-to-end test, you can use the provided example script `examples/sftp_e2e_test.py`.

### Prerequisites

-   Docker
-   `paramiko` library

### Steps

1.  Start a local SFTP server using Docker:

    ```bash
    docker run -p 2222:22 -d atmoz/sftp foo:pass:::upload
    ```

2.  Run the example script:

    ```bash
    python examples/sftp_e2e_test.py
    ```

This script will:
1.  Upload test data to the SFTP server using `paramiko`.
2.  Read the data using the `sftp` data source.
3.  Write the data back to the SFTP server.
4.  Verify the output files.

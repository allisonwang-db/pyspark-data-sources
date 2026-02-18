# SFTP Data Source

The SFTP Data Source allows you to read and write text files directly from/to an SFTP server using Apache Spark.

## Installation

```bash
pip install pyspark-data-sources[sftp]
```

## Usage

### Read from SFTP

```python
from pyspark_datasources import SFTPDataSource

spark.dataSource.register(SFTPDataSource)

df = spark.read.format("sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "user") \
    .option("password", "pass") \
    .option("path", "/path/to/remote/file.txt") \
    .load()

df.show()
```

### Write to SFTP

```python
df.write.format("sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "user") \
    .option("password", "pass") \
    .option("path", "/path/to/remote/output_dir") \
    .save()
```

## Options

| Option | Description | Required | Default |
| :--- | :--- | :--- | :--- |
| `host` | The hostname or IP address of the SFTP server. | Yes | - |
| `port` | The port number of the SFTP server. | No | `22` |
| `username` | The username for authentication. | Yes | - |
| `password` | The password for authentication. | No | - |
| `key_filename` | Path to a private key file for authentication. | No | - |
| `path` | The directory or file path on the SFTP server. | Yes | - |
| `recursive` | Whether to recursively list files in directories (Read only). | No | `false` |

**Note**: Either `password` or `key_filename` must be provided.

## Authentication

You can authenticate using either a password or a private key file.

### Password Authentication

```python
.option("password", "your_password")
```

### Private Key Authentication

```python
.option("key_filename", "/path/to/private/key")
```

## Reading Multiple Files

If the `path` option points to a directory, the connector will read all files in that directory.

```python
df = spark.read.format("sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "user") \
    .option("password", "pass") \
    .option("path", "/path/to/directory") \
    .load()
```

To read files from subdirectories recursively, set `recursive` to `true`.

```python
df = spark.read.format("sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "user") \
    .option("password", "pass") \
    .option("path", "/path/to/directory") \
    .option("recursive", "true") \
    .load()
```

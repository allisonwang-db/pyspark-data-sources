from typing import List, Iterator, Union
import os
import glob

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType


class ArrowDataSource(DataSource):
    """
    A data source for reading Apache Arrow files (.arrow, .parquet) using PyArrow.

    This data source supports reading Arrow/Parquet files from local filesystem or
    cloud storage, leveraging PyArrow's efficient columnar format and returning
    PyArrow RecordBatch objects for optimal performance with PySpark's Arrow integration.

    Name: `arrow`

    Options
    -------
    - `format` : str, default "auto"
        File format: "arrow", "parquet", or "auto" to detect from extension.

    Path Support
    -----------
    Supports various path patterns in the load() method:
    - Single file: "/path/to/file.arrow" or "/path/to/file.parquet"
    - Glob patterns: "/path/to/*.arrow" or "/path/to/data*.parquet"
    - Directory: "/path/to/directory" (reads all .arrow/.parquet files)

    Partitioning Strategy
    --------------------
    The data source creates one partition per file for parallel processing:
    - Single file: 1 partition
    - Multiple files: N partitions (one per file)
    - Directory: N partitions (one per .arrow/.parquet file found)
    
    This enables Spark to process multiple files in parallel across different
    executor cores, improving performance for large datasets.

    Performance Notes
    ----------------
    - Returns PyArrow RecordBatch objects for zero-copy data transfer
    - Leverages PySpark 4.0's enhanced Arrow integration
    - For DataFrames created in Spark, consider using the new df.to_arrow() method
      in PySpark 4.0+ for efficient Arrow conversion

    Examples
    --------
    Register the data source:

    >>> from pyspark_datasources import ArrowDataSource
    >>> spark.dataSource.register(ArrowDataSource)

    Read a single Arrow file:

    >>> df = spark.read.format("arrow").load("/path/to/employees.arrow")
    >>> df.show()
    +---+-----------+---+-------+----------+------+
    | id|       name|age| salary|department|active|
    +---+-----------+---+-------+----------+------+
    |  1|Alice Smith| 28|65000.0|      Tech|  true|
    +---+-----------+---+-------+----------+------+

    Read a single Parquet file:

    >>> df = spark.read.format("arrow").load("/path/to/products.parquet")
    >>> df.show()

    Read multiple files with glob pattern (creates multiple partitions):

    >>> df = spark.read.format("arrow").load("/data/sales/sales_*.arrow")
    >>> df.show()
    >>> print(f"Number of partitions: {df.rdd.getNumPartitions()}")

    Read all Arrow/Parquet files in a directory:

    >>> df = spark.read.format("arrow").load("/data/warehouse/")
    >>> df.show()

    Read with explicit format specification:

    >>> df = spark.read.format("arrow").option("format", "parquet").load("/path/to/data")
    >>> df.show()

    Read mixed Arrow and Parquet files in a directory:

    >>> df = spark.read.format("arrow").load("/path/to/mixed_format_dir/")
    >>> df.show()

    Working with the result DataFrame and PySpark 4.0 Arrow integration:

    >>> df = spark.read.format("arrow").load("/path/to/data.arrow")
    >>> 
    >>> # Process with Spark
    >>> result = df.filter(df.age > 25).groupBy("department").count()
    >>> result.show()
    >>> 
    >>> # Convert back to Arrow using PySpark 4.0+ feature
    >>> arrow_table = result.to_arrow()  # New in PySpark 4.0+
    >>> print(f"Arrow table: {arrow_table}")

    Schema inference example:

    >>> # Schema is automatically inferred from the first file
    >>> df = spark.read.format("arrow").load("/path/to/*.parquet")
    >>> df.printSchema()
    root
     |-- product_id: long (nullable = true)
     |-- product_name: string (nullable = true)
     |-- price: double (nullable = true)
    """

    @classmethod
    def name(cls):
        return "arrow"

    def schema(self) -> StructType:
        path = self.options.get("path")
        if not path:
            raise ValueError("Path option is required for Arrow data source")
        
        # Get the first file to determine schema
        files = self._get_files(path)
        if not files:
            raise ValueError(f"No files found at path: {path}")
        
        # Read schema from first file
        file_format = self._detect_format(files[0])
        if file_format == "parquet":
            table = pq.read_table(files[0])
        else:  # arrow format
            with pa.ipc.open_file(files[0]) as reader:
                table = reader.read_all()
        
        # Convert PyArrow schema to Spark schema using PySpark utility
        from pyspark.sql.pandas.types import from_arrow_schema
        return from_arrow_schema(table.schema)

    def reader(self, schema: StructType) -> "ArrowDataSourceReader":
        return ArrowDataSourceReader(schema, self.options)

    def _get_files(self, path: str) -> List[str]:
        """Get list of files matching the path pattern."""
        if os.path.isfile(path):
            return [path]
        elif os.path.isdir(path):
            # Find all arrow/parquet files in directory
            arrow_files = glob.glob(os.path.join(path, "*.arrow"))
            parquet_files = glob.glob(os.path.join(path, "*.parquet"))
            return sorted(arrow_files + parquet_files)
        else:
            # Treat as glob pattern
            return sorted(glob.glob(path))

    def _detect_format(self, file_path: str) -> str:
        """Detect file format from extension or option."""
        format_option = self.options.get("format", "auto")
        if format_option != "auto":
            return format_option
        
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".parquet":
            return "parquet"
        elif ext in [".arrow", ".ipc"]:
            return "arrow"
        else:
            # Default to arrow format
            return "arrow"


class ArrowDataSourceReader(DataSourceReader):
    """Reader for Arrow data source."""

    def __init__(self, schema: StructType, options: dict) -> None:
        self.schema = schema
        self.options = options
        self.path = options.get("path")
        if not self.path:
            raise ValueError("Path option is required")

    def partitions(self) -> List[InputPartition]:
        """Create partitions, one per file for parallel reading."""
        data_source = ArrowDataSource(self.options)
        files = data_source._get_files(self.path)
        return [InputPartition(file_path) for file_path in files]

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        """Read data from a single file partition, returning PyArrow RecordBatch."""
        file_path = partition.value
        data_source = ArrowDataSource(self.options)
        file_format = data_source._detect_format(file_path)
        
        try:
            if file_format == "parquet":
                # Read Parquet file
                table = pq.read_table(file_path)
                # Convert to RecordBatch iterator
                for batch in table.to_batches():
                    yield batch
            else:
                # Read Arrow IPC file
                with pa.ipc.open_file(file_path) as reader:
                    for i in range(reader.num_record_batches):
                        batch = reader.get_batch(i)
                        yield batch
        except Exception as e:
            raise RuntimeError(f"Failed to read file {file_path}: {str(e)}")
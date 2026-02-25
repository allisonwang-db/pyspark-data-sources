# Data Source Templates

## DataSource Implementation

```python
from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceWriter
from pyspark.sql.types import StructType

class MyDataSource(DataSource):
    """
    A DataSource for reading/writing <Name> data.
    
    Name: `<name>`
    
    Schema: `<schema_definition>`
    """

    @classmethod
    def name(cls):
        return "<name>"

    def schema(self):
        return "<schema_definition>"

    def reader(self, schema: StructType) -> "MyDataSourceReader":
        return MyDataSourceReader(self.options)

    def writer(self, schema: StructType, overwrite: bool) -> "MyDataSourceWriter":
        return MyDataSourceWriter(self.options)
```

## DataSourceReader Implementation

```python
from pyspark.sql.datasource import DataSourceReader
from pyspark.sql.types import Row
from typing import Iterator

class MyDataSourceReader(DataSourceReader):
    def __init__(self, options):
        self.options = options

    def read(self, partition) -> Iterator[Row]:
        # Implement reading logic here
        yield Row(...)
```

## DataSourceWriter Implementation

```python
from pyspark.sql.datasource import DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import Row
from typing import Iterator, List
from dataclasses import dataclass

@dataclass
class MyCommitMessage(WriterCommitMessage):
    records_written: int
    batch_id: int

class MyDataSourceWriter(DataSourceWriter):
    def __init__(self, options):
        self.options = options

    def write(self, iterator: Iterator[Row]) -> MyCommitMessage:
        # Implement writing logic here
        records_written = 0
        for row in iterator:
            # Write row
            records_written += 1
        return MyCommitMessage(records_written=records_written, batch_id=0)

    def commit(self, messages: List[MyCommitMessage]) -> None:
        # Commit logic
        pass

    def abort(self, messages: List[MyCommitMessage]) -> None:
        # Abort logic
        pass
```

## Test Implementation

```python
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark_datasources.<name> import MyDataSource

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_datasource_registration(spark):
    spark.dataSource.register(MyDataSource)
    assert MyDataSource.name() == "<name>"

def test_reader(spark):
    spark.dataSource.register(MyDataSource)
    # Mock external dependencies
    with patch("pyspark_datasources.<name>.ExternalLib") as mock_lib:
        # Setup mock behavior
        df = spark.read.format("<name>").load()
        assert df.count() > 0
```

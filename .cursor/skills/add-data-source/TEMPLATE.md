# Data Source Template

Use this template as a starting point for your new data source.

```python
from typing import Any, Iterator, List
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType

class MyDataSource(DataSource):
    """
    Description of the data source.
    """

    @classmethod
    def name(cls):
        """
        Returns the name of the data source.
        """
        return "my_source"

    def schema(self):
        """
        Returns the default schema or schema inference logic.
        """
        return "value string"

    def reader(self, schema: StructType) -> "DataSourceReader":
        """
        Returns a reader for the data source.
        """
        return MyDataSourceReader(schema, self.options)

class MyDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        # Validate options here
        if "required_option" not in options:
            raise ValueError("Option 'required_option' is required.")

    def partitions(self) -> List[InputPartition]:
        """
        Returns a list of input partitions.
        """
        # Logic to determine partitions
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any]]:
        """
        Reads data from a partition.
        """
        yield ("data",)
```

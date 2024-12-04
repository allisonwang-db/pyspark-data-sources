import lance
import pyarrow as pa

from dataclasses import dataclass
from typing import Iterator, List
from pyspark.sql.datasource import DataSource, DataSourceArrowWriter, WriterCommitMessage
from pyspark.sql.pandas.types import to_arrow_schema


class LanceSink(DataSource):
    """
    Write a Spark DataFrame into Lance format: https://lancedb.github.io/lance/index.html

    Note this requires Spark master branch nightly build to support `DataSourceArrowWriter`.

    Examples
    --------
    Register the data source:

    >>> from pyspark_datasources import LanceSink
    >>> spark.dataSource.register(LanceSink)

    Create a Spark dataframe with 2 partitions:

    >>> df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], schema="id int, value string")

    Save the dataframe in lance format:

    >>> df.write.format("lance").mode("append").save("/tmp/test_lance")
    /tmp/test_lance
    _transactions _versions     data

    Then you can use lance API to read the dataset:

    >>> import lance
    >>> ds = lance.LanceDataset("/tmp/test_lance")
    >>> ds.to_table().to_pandas()
       id
    0   0
    1   1
    2   2

    Notes
    -----
    - Currently this only works with Spark local mode. Cluster mode is not supported.
    """
    @classmethod
    def name(cls) -> str:
        return "lance"

    def writer(self, schema, overwrite: bool):
        if overwrite:
            raise Exception("Overwrite mode is not supported")
        if "path" not in self.options:
            raise Exception("Dataset URI must be specified when calling save()")
        return LanceWriter(schema, overwrite, self.options)


@dataclass
class LanceCommitMessage(WriterCommitMessage):
    fragments: List[lance.FragmentMetadata]


class LanceWriter(DataSourceArrowWriter):
    def __init__(self, schema, overwrite, options):
        self.options = options
        self.schema = schema  # Spark Schema (pyspark.sql.types.StructType)
        self.arrow_schema = to_arrow_schema(schema)  # Arrow schema (pa.StructType)
        self.uri = options["path"]
        assert not overwrite
        self.read_version = self._get_read_version()

    def _get_read_version(self):
        try:
            ds = lance.LanceDataset(self.uri)
            return ds.version
        except Exception:
            return None

    def write(self, iterator: Iterator[pa.RecordBatch]):
        reader = pa.RecordBatchReader.from_batches(self.arrow_schema, iterator)
        fragments = lance.fragment.write_fragments(reader, self.uri, schema=self.arrow_schema)
        return LanceCommitMessage(fragments=fragments)

    def commit(self, messages):
        fragments = [fragment for msg in messages for fragment in msg.fragments ]
        if self.read_version:
            # This means the dataset already exists.
            op = lance.LanceOperation.Append(fragments)
        else:
            # Create a new dataset.
            schema = to_arrow_schema(self.schema)
            op = lance.LanceOperation.Overwrite(schema, fragments)
        lance.LanceDataset.commit(self.uri, op, read_version=self.read_version)

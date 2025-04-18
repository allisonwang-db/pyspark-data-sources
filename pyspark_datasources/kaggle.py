import tempfile
from functools import cached_property
from typing import TYPE_CHECKING, Iterator

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    import pyarrow as pa


class KaggleDataSource(DataSource):
    """
    A DataSource for reading Kaggle datasets in Spark.

    This data source allows reading datasets from Kaggle directly into Spark DataFrames.

    Name: `kaggle`

    Options
    -------
    - `handle`: The dataset handle on Kaggle, in the form of `{owner_slug}/{dataset_slug}`
        or `{owner_slug}/{dataset_slug}/versions/{version_number}`
    - `path`: The path to a file within the dataset.
    - `username`: The Kaggle username for authentication.
    - `key`: The Kaggle API key for authentication.

    Notes:
    -----
    - The `kagglehub` library is required to use this data source. Make sure it is installed.
    - To read private datasets or datasets that require user authentication, `username` and `key` must be provided.
    - Currently all data is read from a single partition.

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import KaggleDataSource
    >>> spark.dataSource.register(KaggleDataSource)

    Load a public dataset from Kaggle.

    >>> spark.read.format("kaggle").options(handle="yasserh/titanic-dataset").load("Titanic-Dataset.csv").select("Name").show()
    +--------------------+
    |                Name|
    +--------------------+
    |Braund, Mr. Owen ...|
    |Cumings, Mrs. Joh...|
    |...                 |
    +--------------------+

    Load a private dataset with authentication.

    >>> spark.read.format("kaggle").options(
    ...     username="myaccount",
    ...     key="<token>",
    ...     handle="myaccount/my-private-dataset",
    ... ).load("file.csv").show()
    """

    @classmethod
    def name(cls) -> str:
        return "kaggle"

    @cached_property
    def _data(self) -> "pa.Table":
        import ast
        import os

        import pyarrow as pa

        handle = self.options.pop("handle")
        path = self.options.pop("path")
        username = self.options.pop("username", None)
        key = self.options.pop("key", None)
        if username or key:
            if not (username and key):
                raise ValueError(
                    "Both username and key must be provided to authenticate."
                )
            os.environ["KAGGLE_USERNAME"] = username
            os.environ["KAGGLE_KEY"] = key

        kwargs = {k: ast.literal_eval(v) for k, v in self.options.items()}

        # Cache in a temporary directory to avoid writing to ~ which may be read-only
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["KAGGLEHUB_CACHE"] = tmpdir
            import kagglehub

            df = kagglehub.dataset_load(
                kagglehub.KaggleDatasetAdapter.PANDAS,
                handle,
                path,
                **kwargs,
            )
            return pa.Table.from_pandas(df)

    def schema(self) -> StructType:
        return from_arrow_schema(self._data.schema)

    def reader(self, schema: StructType) -> "KaggleDataReader":
        return KaggleDataReader(self)


class KaggleDataReader(DataSourceReader):
    def __init__(self, source: KaggleDataSource):
        self.source = source

    def read(self, partition) -> Iterator["pa.RecordBatch"]:
        yield from self.source._data.to_batches()

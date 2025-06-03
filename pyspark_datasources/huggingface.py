from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType


class HuggingFaceDatasets(DataSource):
    """
    An example data source for reading HuggingFace Datasets in Spark.

    This data source allows reading public datasets from the HuggingFace Hub directly into Spark
    DataFrames. The schema is automatically inferred from the dataset features. The split can be
    specified using the `split` option. The default split is `train`.

    Name: `huggingface`

    Notes:
    -----
    - Please use the official HuggingFace Datasets API: https://github.com/huggingface/pyspark_huggingface.
    - The HuggingFace `datasets` library is required to use this data source. Make sure it is installed.
    - If the schema is automatically inferred, it will use string type for all fields.
    - Currently it can only be used with public datasets. Private or gated ones are not supported.

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import HuggingFaceDatasets
    >>> spark.dataSource.register(HuggingFaceDatasets)

    Load a public dataset from the HuggingFace Hub.

    >>> spark.read.format("huggingface").load("imdb").show()
    +--------------------+-----+
    |                text|label|
    +--------------------+-----+
    |I rented I AM CUR...|    0|
    |"I Am Curious: Ye...|    0|
    |...                 |  ...|
    +--------------------+-----+

    Load a specific split from a public dataset from the HuggingFace Hub.

    >>> spark.read.format("huggingface").option("split", "test").load("imdb").show()
    +--------------------+-----+
    |                text|label|
    +--------------------+-----+
    |I love sci-fi and...|    0|
    |Worth the enterta...|    0|
    |...                 |  ...|
    +--------------------+-----+
    """

    def __init__(self, options):
        super().__init__(options)
        if "path" not in options or not options["path"]:
            raise Exception("You must specify a dataset name in`.load()`.")

    @classmethod
    def name(cls):
        return "huggingface"

    def schema(self):
        # The imports must be inside the method to be serializable.
        from datasets import load_dataset_builder
        dataset_name = self.options["path"]
        ds_builder = load_dataset_builder(dataset_name)
        features = ds_builder.info.features
        if features is None:
            raise Exception(
                "Unable to automatically determine the schema using the dataset features. "
                "Please specify the schema manually using `.schema()`."
            )
        schema = StructType()
        for key, value in features.items():
            # For simplicity, use string for all values.
            schema.add(StructField(key, StringType(), True))
        return schema

    def reader(self, schema: StructType) -> "DataSourceReader":
        return HuggingFaceDatasetsReader(schema, self.options)


class HuggingFaceDatasetsReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.dataset_name = options["path"]
        # TODO: validate the split value.
        self.split = options.get("split", "train")  # Default using train split.

    def read(self, partition):
        from datasets import load_dataset
        columns = [field.name for field in self.schema.fields]
        iter_dataset = load_dataset(self.dataset_name, split=self.split, streaming=True)
        for example in iter_dataset:
            yield tuple([example.get(column) for column in columns])

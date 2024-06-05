import io
import json
import time

from dataclasses import dataclass
from typing import Dict, List

from pyspark.sql.types import StructType
from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage


class SimpleJsonDataSource(DataSource):
    """
    A simple json writer for writing data to Databricks DBFS.

    Examples
    --------

    >>> import pyspark.sql.functions as sf
    >>> df = spark.range(0, 10, 1, 2).withColumn("value", sf.expr("concat('value_', id)"))

    Register the data source.

    >>> from pyspark_datasources import SimpleJsonDataSource
    >>> spark.dataSource.register(SimpleJsonDataSource)

    Append the DataFrame to a DBFS path as json files.

    >>> (
    ...     df.write.format("simplejson")
    ...     .mode("append")
    ...     .option("databricks_url", "https://your-databricks-instance.cloud.databricks.com")
    ...     .option("databricks_token", "your-token")
    ...     .save("/path/to/output")
    ... )

    Overwrite the DataFrame to a DBFS path as json files.

    >>> (
    ...     df.write.format("simplejson")
    ...     .mode("overwrite")
    ...     .option("databricks_url", "https://your-databricks-instance.cloud.databricks.com")
    ...     .option("databricks_token", "your-token")
    ...     .save("/path/to/output")
    ... )
    """
    @classmethod
    def name(self) -> str:
        return "simplejson"

    def writer(self, schema: StructType, overwrite: bool):
        return SimpleJsonWriter(schema, self.options, overwrite)


@dataclass
class CommitMessage(WriterCommitMessage):
    output_path: str


class SimpleJsonWriter(DataSourceWriter):
    def __init__(self, schema: StructType, options: Dict, overwrite: bool):
        self.overwrite = overwrite
        self.databricks_url = options.get("databricks_url")
        self.databricks_token = options.get("databricks_token")
        if not self.databricks_url or not self.databricks_token:
            raise Exception("Databricks URL and token must be specified")
        self.path = options.get("path")
        if not self.path:
            raise Exception("You must specify an output path")

    def write(self, iterator):
        # Important: Always import non-serializable libraries inside the `write` method.
        from pyspark import TaskContext
        from databricks.sdk import WorkspaceClient

        # Consume all input rows and dump them as json.
        rows = [row.asDict() for row in iterator]
        json_data = json.dumps(rows)
        f = io.BytesIO(json_data.encode('utf-8'))

        context = TaskContext.get()
        id = context.taskAttemptId()
        file_path = f"{self.path}/{id}_{time.time_ns()}.json"

        # Upload to DFBS.
        w = WorkspaceClient(host=self.databricks_url, token=self.databricks_token)
        w.dbfs.upload(file_path, f)

        return CommitMessage(output_path=file_path)

    def commit(self, messages: List[CommitMessage]):
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(host=self.databricks_url, token=self.databricks_token)
        paths = [message.output_path for message in messages]

        if self.overwrite:
            # Remove all files in the current directory except for the newly written files.
            for file in w.dbfs.list(self.path):
                if file.path not in paths:
                    print(f"[Overwrite] Removing file {file.path}")
                    w.dbfs.delete(file.path)

        # Write a success file
        file_path = f"{self.path}/_SUCCESS"
        f = io.BytesIO(b"success")
        w.dbfs.upload(file_path, f, overwrite=True)

    def abort(self, messages: List[CommitMessage]):
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient(host=self.databricks_url, token=self.databricks_token)
        # Clean up the newly written files
        for message in messages:
            if message is not None:
                print(f"[Abort] Removing up partially written files: {message.output_path}")
                w.dbfs.delete(message.output_path)

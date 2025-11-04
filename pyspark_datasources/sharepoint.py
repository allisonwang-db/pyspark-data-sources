import logging
import abc
import json
import asyncio

from dataclasses import dataclass
from typing import Dict, List, Any, Tuple
from pyspark.sql.types import StructType, Row
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage

logger = logging.getLogger(__name__)


class SharepointResource(abc.ABC):
    """
    Abstract base class modelling a Sharepoint resource.

    A resource is a logical collection of items in Sharepoint, such as a Sharepoint List.
    The resource class is responsible for validating required options provided to the datasource,
    converting a Spark row to a Sharepoint record and pushing the record to Sharepoint using the Microsoft Graph API:
    https://github.com/microsoftgraph/msgraph-sdk-python

    Derive from this class to implement a new resource type (e.g. file-uploads for excel, pdf, etc.) for Sharepoint.

    Notes
    -----
    - implementations of abstract methods `convert_row_to_sharepoint_record` and `push_record` should raise exceptions on
      errors. Those are handled by the corresponding logic in the writer.
      This ensures a consistent behaviour across all resource types.
    - implementations of abstract method `push_record` should be asynchronous to allow for concurrent pushing of records
      using the Microsoft Graph API client.
    """

    def __init__(self, options: Dict[str, str], datasource: str):
        self.options = options
        self.datasource = datasource
        super().__init__()

    @classmethod
    @abc.abstractmethod
    def name(cls) -> str:
        """Abstract method to return the name of the resource."""
        raise NotImplementedError

    @abc.abstractmethod
    def required_options(self) -> List[str]:
        """Abstract method to return the required options for the resource."""
        raise NotImplementedError

    @abc.abstractmethod
    def convert_row_to_sharepoint_record(self, row: Row) -> Any:
        """Abstract method to convert a Spark row to a Sharepoint record."""
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    async def push_record(client, record: Any, site_id: str) -> Any:
        """Abstract method to push a record to Sharepoint using the Microsoft Graph API."""
        raise NotImplementedError

    def validate_required_options(self) -> List[bool]:
        """Validates the required options for a resource."""
        return [True if getattr(self, option.rsplit(".", 1)[-1], None) else False for option in self.required_options()]


class ListResource(SharepointResource):
    """
    Resource class for Sharepoint Lists.

    Spark rows are converted to a dictionary representations of Sharepoint List items.
    The conversion is done by a user-provided mapping of Spark columm names to Sharepoint List column names and
    assumes that the fields already match the datatypes of the Sharepoint List columns.

    Individual records are pushed to Sharepoint using the Microsoft Graph API:
    https://github.com/microsoftgraph/msgraph-sdk-python

    Notes
    -----
    - requires an instance of `GraphServiceClient` from the `msgraph-sdk` library.
    - pushing records is done asynchronously using `asyncio` due to the asynchronous nature of the Microsoft Graph API client.
    - further datataype conversion could be implemented in `convert_row_to_sharepoint_record` if necessary.

    Parameters
    ----------
    list_id : str
        The ID of the Sharepoint List to push the records to.
    fields : dict
        A dictionary mapping Spark column names to Sharepoint List column names.
    """

    _required_options = ["list_id", "fields"]

    def __init__(self, options: Dict[str, str], datasource: str):
        self.list_id = options.get(f"{datasource}.{self.name()}.list_id")
        self.fields = json.loads(options.get(f"{datasource}.{self.name()}.fields"))
        super().__init__(options=options, datasource=datasource)

    @classmethod
    def name(cls) -> str:
        """Returns the name of the resource."""
        return "list"

    def required_options(self) -> List[str]:
        """Returns the required options for the resource."""
        return [f"{self.datasource}.{self.name()}.{option}" for option in self._required_options]

    def convert_row_to_sharepoint_record(self, row: Row) -> Dict[str, Any]:
        """Converts a Spark row to a Sharepoint record."""
        record = {}

        for rowfield, listfield in self.fields.items():
            try:
                # assuming fields already match datatypes of list fields
                # further datatype conversions could be handled here
                value = getattr(row, rowfield)
                if value is not None:
                    record[listfield] = value

            except AttributeError as e:
                raise Exception(f"Field '{rowfield}' not found in row with keys {', '.join(row.asDict().keys())}")
            except Exception as e:
                raise Exception(f"Conversion failed for field '{rowfield}' --> '{listfield}': {str(e)}")
        return record

    async def push_record(self, client, record: Dict[str, Any], site_id: str) -> None:
        """Pushes a record to a Sharepoint List using the Microsoft Graph API client."""
        try:
            # import here to avoid serialization issues
            from msgraph.generated.models.list_item import ListItem
            from msgraph.generated.models.field_value_set import FieldValueSet

            body = ListItem(fields=FieldValueSet(additional_data=record))
            await client.sites.by_site_id(site_id).lists.by_list_id(self.list_id).items.post(body)

        except Exception as e:
            logger.warning(f"Pushing record to Sharepoint List with Microsoft Graph API failed: {str(e)}")
            raise


@dataclass
class SharepointCommitMessage(WriterCommitMessage):
    """Commit message for Sharepoint write operations."""

    records_written: int
    records_failed: int
    batch_id: int


class SharepointDataSource(DataSource):
    """
    A Sharepoint (streaming) datasource for PySpark to write data to Sharepoint objects.

    This datasource enables writing (streaming) data from Spark to Sharepoint using the Microsoft Graph API.
    Currently, it supports writing structured data to Sharepoint Lists.

    Note: This is a write-only datasource, not a full bidirectional data source.

    Name: `pyspark.datasource.sharepoint`

    Notes
    -----
    - Requires the `azure-identity` and `msgraph-sdk` libraries for Microsoft Graph API integration
    - **Write-only datasource**: Only supports streaming write operations (no read operations)
    - **Append-only datasource**: Only supports appending data to Sharepoint Lists, not overwriting or deleting existing data
    - Uses Microsoft Entra ID client-credentials authentication
    - Supports writing of individual records concurrently due to asynchronoous handling of requests
    - Implements exactly-once semantics through Spark's checkpoint mechanism
    - If a streaming write job fails and is resumed from the checkpoint,
      it will not overwrite records already written in Sharepoint;
      it resumes from the last committed offset.
      However, if records were written to Sharepoint but not yet committed at the time of failure,
      duplicate records may occur after recovery.

    Parameters
    -------
    client_id : str
        Client ID of the Azure AD application.
    client_secret : str
        Client secret of the Azure AD application.
    tenant_id : str
        Tenant ID of the Azure AD tenant.
    site_id : str
        Microsoft Sharepoint Site ID (e.g., "contoso.sharepoint.com,12345-...-6789,abcdef...")
    batch_size : int
        Number of records to process concurrently using the Microsoft Graph API client (default: 200).
    fail_fast : bool
        Whether to fail the job if a record fails to be written (default: False).
    resource : str
        The type of resource to write to. Currently supported: ["list"].

    Examples
    --------
    Register the Sharepoint Datasource:

    >>> from pyspark_datasources import SharepointDataSource
    >>> spark.dataSource.register(SharepointDataSource)

    Write streaming data to a Sharepoint List:

    >>> import json
    >>> from pyspark.sql import SparkSession
    >>> from pyspark.sql.functions import col, lit
    >>>
    >>> spark = SparkSession.builder.appName("SharepointExample").getOrCreate()
    >>> spark.dataSource.register(SharepointDataSource)
    >>>
    >>> # Create sample streaming data
    >>> streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
    >>> list_data = streaming_df.select(
    ...     col("value").cast("string").alias("name"),
    ...     lit("Technology").alias("industry"),
    ...     (col("value") * 100000).cast("double").alias("annual_revenue")
    ... )
    >>>
    >>> # Write to Sharepoint List using the datasource
    >>> query = list_data.writeStream \\
    ...     .format("pyspark.datasource.sharepoint") \\
    ...     .option("pyspark.datasource.sharepoint.auth.client_id", "your-client-id") \\
    ...     .option("pyspark.datasource.sharepoint.auth.client_secret", "your-client-secret") \\
    ...     .option("pyspark.datasource.sharepoint.auth.tenant_id", "your-tenant-id") \\
    ...     .option("pyspark.datasource.sharepoint.resource", "list") \\
    ...     .option("pyspark.datasource.sharepoint.site_id", "your-site-id") \\
    ...     .option("pyspark.datasource.sharepoint.list.list_id", "your-list-id") \\
    ...     .option("pyspark.datasource.sharepoint.list.fields", json.dumps({"name": "Name", "industry": "Industry", "annual_revenue": "AnnualRevenue"})) \\
    ...     .option("checkpointLocation", "/path/to/checkpoint") \\
    ...     .start()

    Write to Sharepoint List using the datasource and limited concurrency:

    >>> query = list_data.writeStream \\
    ...     .format("pyspark.datasource.sharepoint") \\
    ...     .option("pyspark.datasource.sharepoint.auth.client_id", "your-client-id") \\
    ...     .option("pyspark.datasource.sharepoint.auth.client_secret", "your-client-secret") \\
    ...     .option("pyspark.datasource.sharepoint.auth.tenant_id", "your-tenant-id") \\
    ...     .option("pyspark.datasource.sharepoint.resource", "list") \\
    ...     .option("pyspark.datasource.sharepoint.site_id", "your-site-id") \\
    ...     .option("pyspark.datasource.sharepoint.list.list_id", "your-list-id") \\
    ...     .option("pyspark.datasource.sharepoint.list.fields", json.dumps({"name": "Name", "industry": "Industry", "annual_revenue": "AnnualRevenue"})) \\
    ...     .option("pyspark.datasource.sharepoint.batch_size", "50") \\
    ...     .option("checkpointLocation", "/path/to/checkpoint") \\
    ...     .start()

    Write to Sharepoint List using the datasource and fail in case of any errors:

    >>> query = list_data.writeStream \\
    ...     .format("pyspark.datasource.sharepoint") \\
    ...     .option("pyspark.datasource.sharepoint.auth.client_id", "your-client-id") \\
    ...     .option("pyspark.datasource.sharepoint.auth.client_secret", "your-client-secret") \\
    ...     .option("pyspark.datasource.sharepoint.auth.tenant_id", "your-tenant-id") \\
    ...     .option("pyspark.datasource.sharepoint.resource", "list") \\
    ...     .option("pyspark.datasource.sharepoint.site_id", "your-site-id") \\
    ...     .option("pyspark.datasource.sharepoint.list.list_id", "your-list-id") \\
    ...     .option("pyspark.datasource.sharepoint.list.fields", json.dumps({"name": "Name", "industry": "Industry", "annual_revenue": "AnnualRevenue"})) \\
    ...     .option("pyspark.datasource.sharepoint.fail_fast", True) \\
    ...     .option("checkpointLocation", "/path/to/checkpoint") \\
    ...     .start()

    Key Features:

    - **Write-only datasource**: Designed specifically for writing data to Sharepoint
    - **Stream processing**: Uses Microsoft Graph API for efficient concurrent writes
    - **Exactly-once semantics**: Integrates with Spark's checkpoint mechanism
    - **Error handling**: Control over whether to fail the write operation if a record fails to be written
    - **Flexible resource implementations**: Supports multiple resource types (currently only "list")
    """

    @classmethod
    def name(cls) -> str:
        """Return the short name for this Sharepoint datasource."""
        return "pyspark.datasource.sharepoint"

    def streamWriter(self, schema: StructType, overwrite: bool) -> "SharepointStreamWriter":
        """Create a stream writer for Sharepoint datasource integration."""
        return SharepointStreamWriter(options=self.options, datasource=self.name())


class SharepointStreamWriter(DataSourceStreamWriter):
    """Stream writer implementation for Sharepoint datasource integration."""

    _graph_api_version = "1.0"
    _graph_api_url = "https://graph.microsoft.com"
    _auth_url = "https://login.microsoftonline.com"

    _required_options = ["auth.client_id", "auth.client_secret", "auth.tenant_id", "site_id"]
    _supported_resources = {"list": ListResource}

    def __init__(self, options: Dict[str, str], datasource: str):
        self.options = options

        # sharepoint configuration
        self.client_id = options.get(f"{datasource}.auth.client_id")
        self.client_secret = options.get(f"{datasource}.auth.client_secret")
        self.tenant_id = options.get(f"{datasource}.auth.tenant_id")
        self.scopes = [scope.strip() for scope in options.get(f"{datasource}.auth.scopes", "https://graph.microsoft.com/.default").split(",")]

        self.site_id = options.get(f"{datasource}.site_id")
        self.batch_size = int(options.get(f"{datasource}.batch_size", "200"))
        self.fail_fast = True if str(options.get(f"{datasource}.fail_fast", "false")).strip().lower() == "true" else False

        # resource specific configuration
        resource = self._supported_resources.get(options.get(f"{datasource}.resource", "").lower(), None)
        if resource is None or not issubclass(resource, SharepointResource):
            raise ValueError(f"Unsupported resource: {str(resource)}. Supported resources are: {list(self._supported_resources.keys())}")

        self.resource = resource(options=options, datasource=datasource)

        # validate required options
        if not all([self.client_id, self.client_secret, self.tenant_id, self.site_id, *self.resource.validate_required_options()]):
            raise ValueError(
                f"Sharepoint options \n\t{'\n\t'.join([f'{datasource}.{ro}' for ro in self._required_options])}\nand resource specific options\n\t{'\n\t'.join(self.resource.required_options())}\nare required. "
                "Set them using .option() / .options() method in your streaming query."
            )

        logger.info(f"Initialized Sharepoint stream writer for resource '{self.resource.name()}'")

    def write(self, iterator) -> SharepointCommitMessage:
        """Write data to Sharepoint."""
        # import here to avoid serialization issues
        try:
            from azure.identity import ClientSecretCredential
            from msgraph import GraphServiceClient

        except ImportError:
            raise ImportError(
                "azure-identity and msgraph-sdk libraries are required for Sharepoint integration. "
                "Install it with: pip install azure-identity msgraph-sdk"
            )

        from pyspark import TaskContext

        # get task context for batch identification
        context = TaskContext.get()
        batch_id = context.taskAttemptId()

        # connect to Microsoft Graph API
        try:
            credentials = ClientSecretCredential(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            client = GraphServiceClient(credentials=credentials, scopes=self.scopes)
            logger.info(f"Connected to Microsoft Graph API (batch {batch_id})")

        except Exception as e:
            logger.error(f"Could not connect to Microsoft Graph API (batch {batch_id})")
            raise ConnectionError(f"Microsoft Graph API connection failed: {str(e)}")

        # convert rows to sharepoint records and write in batches to avoid memory issues
        buffer = []
        total_records_written = 0
        total_records_failed = []

        def flush_buffer():
            nonlocal total_records_written
            nonlocal total_records_failed
            if buffer:
                success, failed = self._push_to_sharepoint(client=client, records=buffer, batch_id=batch_id)
                total_records_written += success
                total_records_failed.extend(failed)
                buffer.clear()

        for row in iterator:
            try:
                record = self.resource.convert_row_to_sharepoint_record(row=row)
            except Exception as e:
                record = None
                total_records_failed.append(Exception(f"Could not convert row to Sharepoint record (batch {batch_id}):\n\t\t{str(e)}"))

            if record:  # only add non-empty records
                buffer.append(record)
                if len(buffer) >= self.batch_size:
                    flush_buffer()

        # flush any remaining records in the buffer
        if buffer:
            flush_buffer()

        if total_records_failed:
            msg = f"Failed to write {len(total_records_failed)} records to Sharepoint (batch {batch_id}):\n\t{'\n\t'.join([str(f) for f in total_records_failed])}"
            if self.fail_fast:
                raise Exception(msg)
            else:
                logger.warning(msg)

        return SharepointCommitMessage(records_written=total_records_written, records_failed=len(total_records_failed), batch_id=batch_id)

    def _push_to_sharepoint(self, client, records: List[Dict[str, Any]], batch_id: int) -> Tuple[int, List[Any]]:
        """Push records to Sharepoint using Microsoft Graph API."""

        async def push() -> List[Any]:
            sem = asyncio.Semaphore(self.batch_size)

            async def task(i: int, record: Dict[str, Any]):
                async with sem:
                    try:
                        await self.resource.push_record(
                            client=client,
                            record=record,
                            site_id=self.site_id
                        )
                    except Exception as e:
                        raise Exception(f"Task {i}: Failed to write record to Sharepoint (batch {batch_id}):\n\t\t{str(e)}")

            results = await asyncio.gather(
                *(task(i=i, record=record) for i, record in enumerate(records)),
                return_exceptions=True
            )
            return results

        results = asyncio.run(push())
        failed = [result for result in results if isinstance(result, Exception)]
        success = len(records) - len(failed)

        return success, failed

import logging
from dataclasses import dataclass
from typing import Dict, List, Any

from pyspark.sql.types import StructType
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, WriterCommitMessage

logger = logging.getLogger(__name__)


@dataclass
class SalesforceCommitMessage(WriterCommitMessage):
    """Commit message for Salesforce write operations."""

    records_written: int
    batch_id: int


class SalesforceDataSource(DataSource):
    """
    A Salesforce streaming datasource for PySpark to write data to Salesforce objects.

    This datasource enables writing streaming data from Spark to Salesforce using the
    Salesforce REST API. It supports common Salesforce objects like Account, Contact,
    Opportunity, and custom objects.

    Note: This is a write-only datasource, not a full bidirectional data source.

    Name: `salesforce`

    Notes
    -----
    - Requires the `simple-salesforce` library for Salesforce API integration
    - **Write-only datasource**: Only supports streaming write operations (no read operations)
    - Uses Salesforce username/password/security token authentication
    - Supports batch writing with Salesforce Composite Tree API for efficient processing
    - Implements exactly-once semantics through Spark's checkpoint mechanism
    - If a streaming write job fails and is resumed from the checkpoint,
      it will not overwrite records already written in Salesforce;
      it resumes from the last committed offset.
      However, if records were written to Salesforce but not yet committed at the time of failure,
      duplicate records may occur after recovery.

    Parameters
    ----------
    username : str
        Salesforce username (email address)
    password : str
        Salesforce password
    security_token : str
        Salesforce security token (obtained from Salesforce setup)
    salesforce_object : str, optional
        Target Salesforce object name (default: "Account")
    batch_size : str, optional
        Number of records to process per batch (default: "200")
    instance_url : str, optional
        Custom Salesforce instance URL (auto-detected if not provided)
    schema : str, optional
        Custom schema definition for the Salesforce object. If not provided,
        uses the default Account schema. Should be in Spark SQL DDL format.
        Example: "Name STRING NOT NULL, Industry STRING, AnnualRevenue DOUBLE"

    Examples
    --------
    Register the Salesforce Datasource:

    >>> from pyspark_datasources import SalesforceDataSource
    >>> spark.dataSource.register(SalesforceDataSource)

    Write streaming data to Salesforce Accounts:

    >>> from pyspark.sql import SparkSession
    >>> from pyspark.sql.functions import col, lit
    >>> 
    >>> spark = SparkSession.builder.appName("SalesforceExample").getOrCreate()
    >>> spark.dataSource.register(SalesforceDataSource)
    >>> 
    >>> # Create sample streaming data
    >>> streaming_df = spark.readStream.format("rate").load()
    >>> account_data = streaming_df.select(
    ...     col("value").cast("string").alias("Name"),
    ...     lit("Technology").alias("Industry"),
    ...     (col("value") * 100000).cast("double").alias("AnnualRevenue")
    ... )
    >>> 
    >>> # Write to Salesforce using the datasource
    >>> query = account_data.writeStream \\
    ...     .format("pyspark.datasource.salesforce") \\
    ...     .option("username", "your-username@company.com") \\
    ...     .option("password", "your-password") \\
    ...     .option("security_token", "your-security-token") \\
    ...     .option("salesforce_object", "Account") \\
    ...     .option("batch_size", "100") \\
    ...     .option("checkpointLocation", "/path/to/checkpoint") \\
    ...     .start()

    Write to Salesforce Contacts:

    >>> contact_data = streaming_df.select(
    ...     col("value").cast("string").alias("FirstName"),
    ...     lit("Doe").alias("LastName"),
    ...     lit("contact@example.com").alias("Email")
    ... )
    >>> 
    >>> query = contact_data.writeStream \\
    ...     .format("pyspark.datasource.salesforce") \\
    ...     .option("username", "your-username@company.com") \\
    ...     .option("password", "your-password") \\
    ...     .option("security_token", "your-security-token") \\
    ...     .option("salesforce_object", "Contact") \\
    ...     .option("checkpointLocation", "/path/to/checkpoint") \\
    ...     .start()

    Write to custom Salesforce objects:

    >>> custom_data = streaming_df.select(
    ...     col("value").cast("string").alias("Custom_Field__c"),
    ...     lit("Custom Value").alias("Another_Field__c")
    ... )
    >>> 
    >>> query = custom_data.writeStream \\
    ...     .format("pyspark.datasource.salesforce") \\
    ...     .option("username", "your-username@company.com") \\
    ...     .option("password", "your-password") \\
    ...     .option("security_token", "your-security-token") \\
    ...     .option("salesforce_object", "Custom_Object__c") \\
    ...     .option("checkpointLocation", "/path/to/checkpoint") \\
    ...     .start()

    Using custom schema for specific Salesforce objects:

    >>> # Define schema for Contact object as a DDL string
    >>> contact_schema = "FirstName STRING NOT NULL, LastName STRING NOT NULL, Email STRING, Phone STRING"
    >>>
    >>> query = contact_data.writeStream \\
    ...     .format("pyspark.datasource.salesforce") \\
    ...     .option("username", "your-username@company.com") \\
    ...     .option("password", "your-password") \\
    ...     .option("security_token", "your-security-token") \\
    ...     .option("salesforce_object", "Contact") \\
    ...     .option("schema", "FirstName STRING NOT NULL, LastName STRING NOT NULL, Email STRING, Phone STRING") \\
    ...     .option("batch_size", "50") \\
    ...     .option("checkpointLocation", "/path/to/checkpoint") \\
    ...     .start()

    Using schema with Opportunity object:

    >>> opportunity_data = streaming_df.select(
    ...     col("name").alias("Name"),
    ...     col("amount").alias("Amount"),
    ...     col("stage").alias("StageName"),
    ...     col("close_date").alias("CloseDate")
    ... )
    >>> 
    >>> query = opportunity_data.writeStream \\
    ...     .format("pyspark.datasource.salesforce") \\
    ...     .option("username", "your-username@company.com") \\
    ...     .option("password", "your-password") \\
    ...     .option("security_token", "your-security-token") \\
    ...     .option("salesforce_object", "Opportunity") \\
    ...     .option("schema", "Name STRING NOT NULL, Amount DOUBLE, StageName STRING NOT NULL, CloseDate DATE") \\
    ...     .option("checkpointLocation", "/path/to/checkpoint") \\
    ...     .start()
    
    Key Features:
    
    - **Write-only datasource**: Designed specifically for writing data to Salesforce
    - **Batch processing**: Uses Salesforce Composite Tree API for efficient bulk writes
    - **Exactly-once semantics**: Integrates with Spark's checkpoint mechanism
    - **Error handling**: Graceful fallback to individual record creation if batch fails
    - **Flexible schema**: Supports any Salesforce object with custom schema definition
    """

    @classmethod
    def name(cls) -> str:
        """Return the short name for this Salesforce datasource."""
        return "pyspark.datasource.salesforce"

    def schema(self) -> str:
        """
        Return the schema for Salesforce objects.

        If the user provides a 'schema' option, use it.
        Otherwise, return the default Account schema.
        """
        user_schema = self.options.get("schema")
        if user_schema:
            return user_schema
        return """
            Name STRING NOT NULL,
            Industry STRING,
            Phone STRING,
            Website STRING,
            AnnualRevenue DOUBLE,
            NumberOfEmployees INT,
            BillingStreet STRING,
            BillingCity STRING,
            BillingState STRING,
            BillingPostalCode STRING,
            BillingCountry STRING
        """

    def streamWriter(self, schema: StructType, overwrite: bool) -> "SalesforceStreamWriter":
        """Create a stream writer for Salesforce datasource integration."""
        return SalesforceStreamWriter(schema, self.options)


class SalesforceStreamWriter(DataSourceStreamWriter):
    """Stream writer implementation for Salesforce datasource integration."""

    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options

        # Extract Salesforce configuration
        self.username = options.get("username")
        self.password = options.get("password")
        self.security_token = options.get("security_token")
        self.instance_url = options.get("instance_url")
        self.salesforce_object = options.get("salesforce_object", "Account")
        self.batch_size = int(options.get("batch_size", "200"))

        # Validate required options
        if not all([self.username, self.password, self.security_token]):
            raise ValueError(
                "Salesforce username, password, and security_token are required. "
                "Set them using .option() method in your streaming query."
            )

        logger.info(f"Initializing Salesforce writer for object '{self.salesforce_object}'")

    def write(self, iterator) -> SalesforceCommitMessage:
        """Write data to Salesforce."""
        # Import here to avoid serialization issues
        try:
            from simple_salesforce import Salesforce
        except ImportError:
            raise ImportError(
                "simple-salesforce library is required for Salesforce integration. "
                "Install it with: pip install simple-salesforce"
            )

        from pyspark import TaskContext

        # Get task context for batch identification
        context = TaskContext.get()
        batch_id = context.taskAttemptId()

        # Connect to Salesforce
        try:
            sf_kwargs = {
                "username": self.username,
                "password": self.password,
                "security_token": self.security_token,
            }
            if self.instance_url:
                sf_kwargs["instance_url"] = self.instance_url

            sf = Salesforce(**sf_kwargs)
            logger.info(f"✓ Connected to Salesforce (batch {batch_id})")
        except Exception as e:
            logger.error(f"Failed to connect to Salesforce: {str(e)}")
            raise ConnectionError(f"Salesforce connection failed: {str(e)}")

        # Convert rows to Salesforce records and write in batches to avoid memory issues
        records_buffer = []
        total_records_written = 0

        def flush_buffer():
            nonlocal total_records_written
            if records_buffer:
                try:
                    written = self._write_to_salesforce(sf, records_buffer, batch_id)
                    logger.info(
                        f"✅ Batch {batch_id}: Successfully wrote {written} records (buffer flush)"
                    )
                    total_records_written += written
                except Exception as e:
                    logger.error(
                        f"❌ Batch {batch_id}: Failed to write records during buffer flush: {str(e)}"
                    )
                    raise
                records_buffer.clear()

        for row in iterator:
            try:
                record = self._convert_row_to_salesforce_record(row)
                if record:  # Only add non-empty records
                    records_buffer.append(record)
                    if len(records_buffer) >= self.batch_size:
                        flush_buffer()
            except Exception as e:
                logger.warning(f"Failed to convert row to Salesforce record: {str(e)}")

        # Flush any remaining records in the buffer
        if records_buffer:
            flush_buffer()

        if total_records_written == 0:
            logger.info(f"No valid records to write in batch {batch_id}")
        else:
            logger.info(
                f"✅ Batch {batch_id}: Successfully wrote {total_records_written} records (total)"
            )

        return SalesforceCommitMessage(records_written=total_records_written, batch_id=batch_id)

    def _convert_row_to_salesforce_record(self, row) -> Dict[str, Any]:
        """Convert a Spark Row to a Salesforce record format."""
        record = {}

        for field in self.schema.fields:
            field_name = field.name
            try:
                # Use getattr for safe field access
                value = getattr(row, field_name, None)

                if value is not None:
                    # Convert value based on field type
                    if hasattr(value, "isoformat"):  # datetime objects
                        record[field_name] = value.isoformat()
                    elif isinstance(value, (int, float)):
                        record[field_name] = value
                    else:
                        record[field_name] = str(value)

            except Exception as e:
                logger.warning(f"Failed to convert field '{field_name}': {str(e)}")

        return record

    def _write_to_salesforce(self, sf, records: List[Dict[str, Any]], batch_id: int) -> int:
        """Write records to Salesforce using REST API."""
        success_count = 0

        # Process records in batches using sObject Collections API
        for i in range(0, len(records), self.batch_size):
            batch_records = records[i : i + self.batch_size]

            try:
                # Use Composite Tree API for batch creation (up to 200 records)
                # Prepare records for batch API
                collection_records = []
                for idx, record in enumerate(batch_records):
                    # Add required attributes for Composite Tree API
                    record_with_attributes = {
                        "attributes": {
                            "type": self.salesforce_object,
                            "referenceId": f"ref{i + idx}",
                        },
                        **record,
                    }
                    collection_records.append(record_with_attributes)

                # Make batch API call using Composite Tree API
                # This API is specifically designed for batch inserts
                payload = {"records": collection_records}

                response = sf.restful(
                    f"composite/tree/{self.salesforce_object}", method="POST", json=payload
                )

                # Count successful records
                # Composite Tree API returns a different response format
                if isinstance(response, dict):
                    # Check if the batch was successful
                    if response.get("hasErrors", True) is False:
                        # All records in the batch were created successfully
                        success_count += len(batch_records)
                    else:
                        # Some records failed, check individual results
                        results = response.get("results", [])
                        for result in results:
                            if "id" in result:
                                success_count += 1
                            else:
                                errors = result.get("errors", [])
                                for error in errors:
                                    logger.warning(
                                        f"Failed to create record {result.get('referenceId', 'unknown')}: {error.get('message', 'Unknown error')}"
                                    )
                else:
                    logger.error(f"Unexpected response format: {response}")

            except Exception as e:
                logger.error(
                    f"Error in batch creation for batch {i//self.batch_size + 1}: {str(e)}"
                )
                # Fallback to individual record creation for this batch
                try:
                    sf_object = getattr(sf, self.salesforce_object)
                    for j, record in enumerate(batch_records):
                        try:
                            # Create the record in Salesforce
                            result = sf_object.create(record)

                            if result.get("success"):
                                success_count += 1
                            else:
                                logger.warning(
                                    f"Failed to create record {i+j}: {result.get('errors', 'Unknown error')}"
                                )

                        except Exception as e:
                            logger.error(f"Error creating record {i+j}: {str(e)}")
                except AttributeError:
                    raise ValueError(f"Salesforce object '{self.salesforce_object}' not found")

            # Log progress for large batches
            if len(records) > 50 and (i + self.batch_size) % 100 == 0:
                logger.info(
                    f"Batch {batch_id}: Processed {i + self.batch_size}/{len(records)} records"
                )

        return success_count

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
    A Salesforce streaming data source for PySpark to write data to Salesforce objects.

    This data source enables writing streaming data from Spark to Salesforce using the
    Salesforce REST API. It supports common Salesforce objects like Account, Contact,
    Opportunity, and custom objects.

    Name: `salesforce`

    Notes
    -----
    - Requires the `simple-salesforce` library for Salesforce API integration
    - Only supports streaming write operations (not read operations)
    - Uses Salesforce username/password/security token authentication
    - Supports streaming processing for efficient API usage

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

    Examples
    --------
    Register the data source:

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
    >>> # Write to Salesforce
    >>> query = account_data.writeStream \\
    ...     .format("salesforce") \\
    ...     .option("username", "your-username@company.com") \\
    ...     .option("password", "your-password") \\
    ...     .option("security_token", "your-security-token") \\
    ...     .option("salesforce_object", "Account") \\
    ...     .option("batch_size", "100") \\
    ...     .start()

    Write to Salesforce Contacts:

    >>> contact_data = streaming_df.select(
    ...     col("value").cast("string").alias("FirstName"),
    ...     lit("Doe").alias("LastName"),
    ...     lit("contact@example.com").alias("Email")
    ... )
    >>> 
    >>> query = contact_data.writeStream \\
    ...     .format("salesforce") \\
    ...     .option("username", "your-username@company.com") \\
    ...     .option("password", "your-password") \\
    ...     .option("security_token", "your-security-token") \\
    ...     .option("salesforce_object", "Contact") \\
    ...     .start()

    Write to custom Salesforce objects:

    >>> custom_data = streaming_df.select(
    ...     col("value").cast("string").alias("Custom_Field__c"),
    ...     lit("Custom Value").alias("Another_Field__c")
    ... )
    >>> 
    >>> query = custom_data.writeStream \\
    ...     .format("salesforce") \\
    ...     .option("username", "your-username@company.com") \\
    ...     .option("password", "your-password") \\
    ...     .option("security_token", "your-security-token") \\
    ...     .option("salesforce_object", "Custom_Object__c") \\
    ...     .start()
    """

    @classmethod
    def name(cls) -> str:
        """Return the short name for this data source."""
        return "salesforce"

    def schema(self) -> str:
        """
        Define the default schema for Salesforce Account objects.
        
        This schema can be overridden by users when creating their DataFrame.
        """
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
        """Create a stream writer for Salesforce integration."""
        return SalesforceStreamWriter(schema, self.options)


class SalesforceStreamWriter(DataSourceStreamWriter):
    """Stream writer implementation for Salesforce integration."""

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
                'username': self.username,
                'password': self.password,
                'security_token': self.security_token
            }
            if self.instance_url:
                sf_kwargs['instance_url'] = self.instance_url
                
            sf = Salesforce(**sf_kwargs)
            logger.info(f"✓ Connected to Salesforce (batch {batch_id})")
        except Exception as e:
            logger.error(f"Failed to connect to Salesforce: {str(e)}")
            raise ConnectionError(f"Salesforce connection failed: {str(e)}")
        
        # Convert rows to Salesforce records
        records = []
        for row in iterator:
            try:
                record = self._convert_row_to_salesforce_record(row)
                if record:  # Only add non-empty records
                    records.append(record)
            except Exception as e:
                logger.warning(f"Failed to convert row to Salesforce record: {str(e)}")
        
        if not records:
            logger.info(f"No valid records to write in batch {batch_id}")
            return SalesforceCommitMessage(records_written=0, batch_id=batch_id)
        
        # Write records to Salesforce
        try:
            records_written = self._write_to_salesforce(sf, records, batch_id)
            logger.info(f"✅ Batch {batch_id}: Successfully wrote {records_written} records")
            return SalesforceCommitMessage(records_written=records_written, batch_id=batch_id)
        except Exception as e:
            logger.error(f"❌ Batch {batch_id}: Failed to write records: {str(e)}")
            raise

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
                    if hasattr(value, 'isoformat'):  # datetime objects
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
        
        # Get the Salesforce object API
        try:
            sf_object = getattr(sf, self.salesforce_object)
        except AttributeError:
            raise ValueError(f"Salesforce object '{self.salesforce_object}' not found")
        
        # Process records in batches
        for i in range(0, len(records), self.batch_size):
            batch_records = records[i:i + self.batch_size]
            
            for j, record in enumerate(batch_records):
                try:
                    # Create the record in Salesforce
                    result = sf_object.create(record)
                    
                    if result.get('success'):
                        success_count += 1
                    else:
                        logger.warning(f"Failed to create record {i+j}: {result.get('errors', 'Unknown error')}")
                        
                except Exception as e:
                    logger.error(f"Error creating record {i+j}: {str(e)}")
            
            # Log progress for large batches
            if len(records) > 50 and (i + self.batch_size) % 100 == 0:
                logger.info(f"Batch {batch_id}: Processed {i + self.batch_size}/{len(records)} records")
        
        return success_count

    def commit(self, messages: List[SalesforceCommitMessage], batch_id: int) -> None:
        """Commit the write operation."""
        total_records = sum(msg.records_written for msg in messages if msg is not None)
        total_batches = len([msg for msg in messages if msg is not None])
        
        logger.info(f"✅ Commit batch {batch_id}: Successfully wrote {total_records} records across {total_batches} batches")

    def abort(self, messages: List[SalesforceCommitMessage], batch_id: int) -> None:
        """Abort the write operation."""
        total_batches = len([msg for msg in messages if msg is not None])
        logger.warning(f"❌ Abort batch {batch_id}: Rolling back {total_batches} batches")
        # Note: Salesforce doesn't support transaction rollback for individual records
        # Records that were successfully created will remain in Salesforce

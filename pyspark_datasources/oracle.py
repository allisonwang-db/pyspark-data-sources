from dataclasses import dataclass
from typing import Any, Iterator, List, Optional

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    DataSourceWriter,
    InputPartition,
    WriterCommitMessage,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _get_connection(options):
    try:
        import oracledb
    except ImportError:
        raise ImportError(
            "oracledb is required for Oracle data source. Install it with `pip install oracledb`."
        )

    user = options.get("user")
    if not user:
        raise ValueError("Option 'user' is required.")

    password = options.get("password")
    if not password:
        raise ValueError("Option 'password' is required.")

    dsn = options.get("dsn")
    if not dsn:
        host = options.get("host", "localhost")
        port = options.get("port", "1521")
        sid = options.get("sid")
        service_name = options.get("service_name")

        if sid:
            dsn = oracledb.makedsn(host, port, sid=sid)
        elif service_name:
            dsn = oracledb.makedsn(host, port, service_name=service_name)
        else:
            # Fallback or simple connection string
            dsn = f"{host}:{port}/{service_name if service_name else ''}"

    return oracledb.connect(user=user, password=password, dsn=dsn)


class OracleDataSource(DataSource):
    """
    A data source for reading and writing data to Oracle databases.
    """

    @classmethod
    def name(cls):
        return "oracle"

    def schema(self) -> StructType:
        """
        Infers schema from the table or query.
        """
        conn = _get_connection(self.options)
        cursor = conn.cursor()
        try:
            dbtable = self.options.get("dbtable")
            query = self.options.get("query")

            if dbtable:
                # Use a limit 0 query to get schema
                stmt = f"SELECT * FROM {dbtable} WHERE 1=0"
            elif query:
                stmt = f"SELECT * FROM ({query}) WHERE 1=0"
            else:
                raise ValueError("Either 'dbtable' or 'query' must be provided.")

            cursor.execute(stmt)
            
            # Map Oracle types to Spark types
            # This is a basic mapping and might need refinement
            fields = []
            import oracledb
            
            for col in cursor.description:
                col_name = col[0]
                col_type = col[1]
                
                # Default to StringType
                spark_type = StringType()
                
                if col_type == oracledb.DB_TYPE_NUMBER:
                    # Could be Integer, Long, Double, or Decimal
                    # Without precision/scale info readily available in simple description, 
                    # Double is a safe bet for generic numbers, or String to preserve precision.
                    # Let's use DoubleType for now as a common approximation.
                    spark_type = DoubleType()
                elif col_type in (oracledb.DB_TYPE_VARCHAR, oracledb.DB_TYPE_CHAR, oracledb.DB_TYPE_CLOB):
                    spark_type = StringType()
                elif col_type in (oracledb.DB_TYPE_DATE, oracledb.DB_TYPE_TIMESTAMP):
                    spark_type = TimestampType()
                
                fields.append(StructField(col_name, spark_type))
                
            return StructType(fields)
        finally:
            cursor.close()
            conn.close()

    def reader(self, schema: StructType) -> "DataSourceReader":
        return OracleDataSourceReader(schema, self.options)

    def writer(self, schema: StructType, overwrite: bool) -> "DataSourceWriter":
        return OracleDataSourceWriter(schema, self.options, overwrite)


class OracleDataSourceReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        self.dbtable = options.get("dbtable")
        self.query = options.get("query")
        
        if not self.dbtable and not self.query:
             raise ValueError("Either 'dbtable' or 'query' must be provided.")

    def partitions(self) -> List[InputPartition]:
        # Basic implementation: Single partition
        # TODO: Implement partitioning based on partitionColumn, lowerBound, upperBound, numPartitions
        return [OracleInputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[tuple[Any]]:
        conn = _get_connection(self.options)
        cursor = conn.cursor()
        try:
            if self.dbtable:
                sql = f"SELECT * FROM {self.dbtable}"
            else:
                sql = self.query
                
            cursor.execute(sql)
            
            for row in cursor:
                yield row
        finally:
            cursor.close()
            conn.close()


@dataclass
class OracleInputPartition(InputPartition):
    id: int


@dataclass
class OracleCommitMessage(WriterCommitMessage):
    partition_id: int
    count: int


class OracleDataSourceWriter(DataSourceWriter):
    def __init__(self, schema, options, overwrite):
        self.schema = schema
        self.options = options
        self.overwrite = overwrite
        self.dbtable = options.get("dbtable")
        
        if not self.dbtable:
            raise ValueError("Option 'dbtable' is required for writing.")

    def write(self, iterator: Iterator[tuple[Any]]) -> OracleCommitMessage:
        conn = _get_connection(self.options)
        cursor = conn.cursor()
        count = 0
        try:
            # Prepare INSERT statement
            # Assuming schema columns match table columns in order
            cols = ",".join(self.schema.names)
            placeholders = ",".join([":" + str(i+1) for i in range(len(self.schema.names))])
            sql = f"INSERT INTO {self.dbtable} ({cols}) VALUES ({placeholders})"
            
            batch_size = 1000
            batch = []
            
            for row in iterator:
                batch.append(row)
                count += 1
                if len(batch) >= batch_size:
                    cursor.executemany(sql, batch)
                    conn.commit()
                    batch = []
            
            if batch:
                cursor.executemany(sql, batch)
                conn.commit()
                
            return OracleCommitMessage(partition_id=0, count=count)
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    def commit(self, messages: List[OracleCommitMessage]) -> None:
        pass

    def abort(self, messages: List[OracleCommitMessage]) -> None:
        pass

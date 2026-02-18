import sys
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark_datasources.oracle import (
    OracleDataSource,
    OracleDataSourceReader,
    OracleDataSourceWriter,
    OracleInputPartition,
    OracleCommitMessage,
)


def test_oracle_schema_inference():
    options = {
        "user": "user",
        "password": "password",
        "dbtable": "mytable"
    }

    mock_oracledb = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    mock_oracledb.connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock DB types
    mock_oracledb.DB_TYPE_NUMBER = "NUMBER"
    mock_oracledb.DB_TYPE_VARCHAR = "VARCHAR"
    
    # Mock cursor description
    mock_cursor.description = [
        ("ID", "NUMBER", None, None, None, None, None),
        ("NAME", "VARCHAR", None, None, None, None, None)
    ]

    with patch.dict(sys.modules, {"oracledb": mock_oracledb}):
        ds = OracleDataSource(options)
        schema = ds.schema()
        
        assert len(schema.fields) == 2
        assert schema.fields[0].name == "ID"
        assert isinstance(schema.fields[0].dataType, DoubleType)
        assert schema.fields[1].name == "NAME"
        assert isinstance(schema.fields[1].dataType, StringType)
        
        # Verify query used for schema inference
        mock_cursor.execute.assert_called_with("SELECT * FROM mytable WHERE 1=0")


def test_oracle_reader_read():
    options = {
        "user": "user",
        "password": "password",
        "dbtable": "mytable"
    }
    
    mock_oracledb = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    mock_oracledb.connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock data
    mock_cursor.__iter__.return_value = iter([(1, "Alice"), (2, "Bob")])
    
    with patch.dict(sys.modules, {"oracledb": mock_oracledb}):
        reader = OracleDataSourceReader(None, options)
        partition = OracleInputPartition(0)
        rows = list(reader.read(partition))
        
        assert len(rows) == 2
        assert rows[0] == (1, "Alice")
        assert rows[1] == (2, "Bob")
        
        mock_cursor.execute.assert_called_with("SELECT * FROM mytable")


def test_oracle_writer_write():
    schema = StructType([
        StructField("id", DoubleType()),
        StructField("name", StringType())
    ])
    
    options = {
        "user": "user",
        "password": "password",
        "dbtable": "mytable"
    }
    
    mock_oracledb = MagicMock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    mock_oracledb.connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    with patch.dict(sys.modules, {"oracledb": mock_oracledb}):
        writer = OracleDataSourceWriter(schema, options, False)
        iterator = iter([(1, "Alice"), (2, "Bob")])
        msg = writer.write(iterator)
        
        assert isinstance(msg, OracleCommitMessage)
        assert msg.count == 2
        
        # Verify insert
        expected_sql = "INSERT INTO mytable (id,name) VALUES (:1,:2)"
        mock_cursor.executemany.assert_called()
        call_args = mock_cursor.executemany.call_args
        assert call_args[0][0] == expected_sql
        # Check batch data
        assert len(call_args[0][1]) == 2


def test_oracle_options_validation():
    mock_oracledb = MagicMock()
    
    with patch.dict(sys.modules, {"oracledb": mock_oracledb}):
        # Missing user - check via schema() which calls _get_connection
        with pytest.raises(ValueError, match="Option 'user' is required"):
            ds = OracleDataSource({"password": "pwd", "dbtable": "t"})
            ds.schema()

        # Test missing dbtable/query for reader
        with pytest.raises(ValueError, match="Either 'dbtable' or 'query' must be provided"):
            OracleDataSourceReader(None, {"user": "u", "password": "p"})
            
        # Test missing dbtable for writer
        with pytest.raises(ValueError, match="Option 'dbtable' is required"):
            OracleDataSourceWriter(None, {"user": "u", "password": "p"}, False)

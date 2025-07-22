import os
import tempfile
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from pyspark.sql import SparkSession
from pyspark_datasources import ArrowDataSource


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


@pytest.fixture
def sample_arrow_file():
    """Create a temporary Arrow file for testing."""
    # Create sample data
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'salary': [50000.0, 60000.0, 75000.0, 55000.0, 68000.0]
    }
    table = pa.table(data)
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.arrow', delete=False) as f:
        with pa.ipc.new_file(f, table.schema) as writer:
            writer.write_table(table)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def sample_parquet_file():
    """Create a temporary Parquet file for testing."""
    # Create sample data
    data = {
        'product_id': [101, 102, 103, 104],
        'product_name': ['Widget A', 'Widget B', 'Gadget X', 'Gadget Y'],
        'price': [19.99, 29.99, 39.99, 49.99],
        'in_stock': [True, False, True, True]
    }
    table = pa.table(data)
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        temp_path = f.name
    
    pq.write_table(table, temp_path)
    
    yield temp_path
    
    # Cleanup
    os.unlink(temp_path)


def test_arrow_datasource_read_arrow_file(spark, sample_arrow_file):
    """Test reading a single Arrow file."""
    spark.dataSource.register(ArrowDataSource)
    df = spark.read.format("arrow").option("path", sample_arrow_file).load()
    
    # Verify data
    assert df.count() == 5
    assert len(df.columns) == 4
    assert set(df.columns) == {'id', 'name', 'age', 'salary'}
    
    # Check some data values
    rows = df.collect()
    assert rows[0]['name'] == 'Alice'
    assert rows[0]['age'] == 25


def test_arrow_datasource_read_parquet_file(spark, sample_parquet_file):
    """Test reading a single Parquet file."""
    spark.dataSource.register(ArrowDataSource)
    df = spark.read.format("arrow").option("path", sample_parquet_file).load()
    
    # Verify data
    assert df.count() == 4
    assert len(df.columns) == 4
    assert set(df.columns) == {'product_id', 'product_name', 'price', 'in_stock'}
    
    # Check some data values
    rows = df.collect()
    assert rows[0]['product_name'] == 'Widget A'
    assert rows[0]['price'] == 19.99


def test_arrow_datasource_explicit_format(spark, sample_parquet_file):
    """Test reading with explicit format specification."""
    spark.dataSource.register(ArrowDataSource)
    df = spark.read.format("arrow").option("path", sample_parquet_file).option("format", "parquet").load()
    
    # Verify data
    assert df.count() == 4
    assert len(df.columns) == 4


def test_arrow_datasource_schema_inference(spark, sample_arrow_file):
    """Test schema inference from Arrow file."""
    spark.dataSource.register(ArrowDataSource)
    
    # Create datasource instance to test schema method
    options = {"path": sample_arrow_file}
    datasource = ArrowDataSource(options)
    schema = datasource.schema()
    
    # Verify schema has expected fields
    field_names = [field.name for field in schema.fields]
    assert 'id' in field_names
    assert 'name' in field_names
    assert 'age' in field_names
    assert 'salary' in field_names


def test_arrow_datasource_missing_path(spark):
    """Test error handling when path is missing."""
    spark.dataSource.register(ArrowDataSource)
    
    with pytest.raises(Exception) as exc_info:
        spark.read.format("arrow").load()
    
    assert "Path option is required" in str(exc_info.value)


def test_arrow_datasource_nonexistent_path(spark):
    """Test error handling for nonexistent path."""
    spark.dataSource.register(ArrowDataSource)
    
    with pytest.raises(Exception) as exc_info:
        spark.read.format("arrow").option("path", "/nonexistent/path.arrow").load()
    
    assert "No files found" in str(exc_info.value)
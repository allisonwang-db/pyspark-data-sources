import os
import pytest

from pyspark.sql import SparkSession
from pyspark_datasources import ArrowDataSource


@pytest.fixture
def spark():
    spark = SparkSession.builder.getOrCreate()
    yield spark


@pytest.fixture
def test_data_dir():
    """Get the path to persistent test data directory."""
    return os.path.join(os.path.dirname(__file__), "data")


def test_arrow_datasource_read_arrow_file(spark, test_data_dir):
    """Test reading a single Arrow file."""
    spark.dataSource.register(ArrowDataSource)
    arrow_file = os.path.join(test_data_dir, "employees.arrow")
    df = spark.read.format("arrow").option("path", arrow_file).load()
    
    # Verify data
    assert df.count() == 5
    assert len(df.columns) == 6
    expected_columns = {'id', 'name', 'age', 'salary', 'department', 'active'}
    assert set(df.columns) == expected_columns
    
    # Check some data values
    rows = df.collect()
    assert rows[0]['name'] == 'Alice Johnson'
    assert rows[0]['age'] == 28
    assert rows[0]['department'] == 'Engineering'


def test_arrow_datasource_read_parquet_file(spark, test_data_dir):
    """Test reading a single Parquet file."""
    spark.dataSource.register(ArrowDataSource)
    parquet_file = os.path.join(test_data_dir, "products.parquet")
    df = spark.read.format("arrow").option("path", parquet_file).load()
    
    # Verify data
    assert df.count() == 6
    assert len(df.columns) == 6
    expected_columns = {'product_id', 'product_name', 'category', 'price', 'in_stock', 'rating'}
    assert set(df.columns) == expected_columns
    
    # Check some data values
    rows = df.collect()
    assert rows[0]['product_name'] == 'Laptop Pro'
    assert rows[0]['price'] == 1299.99
    assert rows[0]['category'] == 'Computer'


def test_arrow_datasource_multiple_partitions(spark, test_data_dir):
    """Test reading multiple Arrow files (multiple partitions)."""
    spark.dataSource.register(ArrowDataSource)
    sales_dir = os.path.join(test_data_dir, "sales", "*.arrow")
    df = spark.read.format("arrow").option("path", sales_dir).load()
    
    # Verify data from all 3 files (Q1, Q2, Q3)
    assert df.count() == 12  # 4 rows per quarter * 3 quarters
    assert len(df.columns) == 5
    expected_columns = {'quarter', 'month', 'sales_id', 'amount', 'region'}
    assert set(df.columns) == expected_columns
    
    # Check that we have data from all quarters
    quarters = [row['quarter'] for row in df.collect()]
    assert 'Q1' in quarters
    assert 'Q2' in quarters
    assert 'Q3' in quarters
    
    # Verify we have 4 rows per quarter
    q1_count = df.filter(df.quarter == 'Q1').count()
    q2_count = df.filter(df.quarter == 'Q2').count()
    q3_count = df.filter(df.quarter == 'Q3').count()
    assert q1_count == 4
    assert q2_count == 4
    assert q3_count == 4


def test_arrow_datasource_directory_read(spark, test_data_dir):
    """Test reading all files in a directory."""
    spark.dataSource.register(ArrowDataSource)
    sales_dir = os.path.join(test_data_dir, "sales")
    df = spark.read.format("arrow").option("path", sales_dir).load()
    
    # Should read all Arrow files in the directory
    assert df.count() == 12
    assert len(df.columns) == 5


def test_arrow_datasource_mixed_formats(spark, test_data_dir):
    """Test reading directory with mixed Arrow and Parquet files."""
    spark.dataSource.register(ArrowDataSource)
    mixed_dir = os.path.join(test_data_dir, "mixed")
    df = spark.read.format("arrow").option("path", mixed_dir).load()
    
    # Should read both Arrow and Parquet files
    assert df.count() == 6  # 3 from each file
    assert len(df.columns) == 3
    expected_columns = {'customer_id', 'name', 'email'}
    assert set(df.columns) == expected_columns
    
    # Verify we have all customer IDs (1-6)
    customer_ids = sorted([row['customer_id'] for row in df.collect()])
    assert customer_ids == [1, 2, 3, 4, 5, 6]


def test_arrow_datasource_explicit_parquet_format(spark, test_data_dir):
    """Test reading with explicit format specification."""
    spark.dataSource.register(ArrowDataSource)
    parquet_file = os.path.join(test_data_dir, "products.parquet")
    df = spark.read.format("arrow").option("path", parquet_file).option("format", "parquet").load()
    
    # Verify data
    assert df.count() == 6
    assert len(df.columns) == 6


def test_arrow_datasource_schema_inference_arrow(spark, test_data_dir):
    """Test schema inference from Arrow file."""
    spark.dataSource.register(ArrowDataSource)
    arrow_file = os.path.join(test_data_dir, "employees.arrow")
    
    # Create datasource instance to test schema method
    options = {"path": arrow_file}
    datasource = ArrowDataSource(options)
    schema = datasource.schema()
    
    # Verify schema has expected fields
    field_names = [field.name for field in schema.fields]
    expected_fields = ['id', 'name', 'age', 'salary', 'department', 'active']
    assert all(field in field_names for field in expected_fields)


def test_arrow_datasource_schema_inference_parquet(spark, test_data_dir):
    """Test schema inference from Parquet file."""
    spark.dataSource.register(ArrowDataSource)
    parquet_file = os.path.join(test_data_dir, "products.parquet")
    
    # Create datasource instance to test schema method
    options = {"path": parquet_file}
    datasource = ArrowDataSource(options)
    schema = datasource.schema()
    
    # Verify schema has expected fields
    field_names = [field.name for field in schema.fields]
    expected_fields = ['product_id', 'product_name', 'category', 'price', 'in_stock', 'rating']
    assert all(field in field_names for field in expected_fields)


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


def test_arrow_datasource_partition_count(spark, test_data_dir):
    """Test that multiple files create multiple partitions."""
    spark.dataSource.register(ArrowDataSource)
    
    # Test with sales directory (3 files)
    options = {"path": os.path.join(test_data_dir, "sales")}
    datasource = ArrowDataSource(options)
    reader = datasource.reader(datasource.schema())
    partitions = reader.partitions()
    
    # Should create 3 partitions (one per file)
    assert len(partitions) == 3
    
    # Verify each partition has a file path
    partition_values = [partition.value for partition in partitions]
    assert all(path.endswith('.arrow') for path in partition_values)
    assert all('sales_q' in path for path in partition_values)


def test_arrow_datasource_direct_file_access():
    """Test direct file access without Spark (for manual verification)."""
    test_data_dir = os.path.join(os.path.dirname(__file__), "data")
    
    # Test Arrow file
    arrow_file = os.path.join(test_data_dir, "employees.arrow")
    options = {"path": arrow_file}
    datasource = ArrowDataSource(options)
    schema = datasource.schema()
    reader = datasource.reader(schema)
    
    # Read data directly
    partitions = reader.partitions()
    assert len(partitions) == 1
    
    # Read from the partition
    data_batches = list(reader.read(partitions[0]))
    assert len(data_batches) > 0
    
    # Verify the data
    batch = data_batches[0]
    assert batch.num_rows == 5
    assert batch.num_columns == 6
    
    print(f"\nDirect access test results:")
    print(f"Schema: {schema}")
    print(f"Data shape: {batch.num_rows} rows, {batch.num_columns} columns")
    print(f"Data preview:\n{batch.to_pandas()}")
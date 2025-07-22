#!/usr/bin/env python3
"""Create persistent test data files for Arrow data source testing."""

import os
import pyarrow as pa
import pyarrow.parquet as pq


def create_test_data_files():
    """Create persistent test data files in the tests directory."""
    tests_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(tests_dir, "data")
    
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Create sample datasets
    
    # Dataset 1: Employee data (Arrow format)
    employee_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Wilson'],
        'age': [28, 34, 42, 26, 31],
        'salary': [65000.0, 78000.0, 85000.0, 58000.0, 72000.0],
        'department': ['Engineering', 'Sales', 'Engineering', 'Marketing', 'Sales'],
        'active': [True, True, False, True, True]
    }
    employee_table = pa.table(employee_data)
    
    # Write as Arrow IPC file
    arrow_path = os.path.join(data_dir, "employees.arrow")
    with pa.ipc.new_file(open(arrow_path, 'wb'), employee_table.schema) as writer:
        writer.write_table(employee_table)
    print(f"Created: {arrow_path}")
    
    # Dataset 2: Product data (Parquet format)
    product_data = {
        'product_id': [101, 102, 103, 104, 105, 106],
        'product_name': ['Laptop Pro', 'Desktop Elite', 'Tablet Max', 'Phone X', 'Watch Sport', 'Headphones Ultra'],
        'category': ['Computer', 'Computer', 'Tablet', 'Phone', 'Wearable', 'Audio'],
        'price': [1299.99, 899.99, 499.99, 799.99, 299.99, 149.99],
        'in_stock': [True, False, True, True, False, True],
        'rating': [4.5, 4.2, 4.7, 4.3, 4.1, 4.6]
    }
    product_table = pa.table(product_data)
    
    # Write as Parquet file
    parquet_path = os.path.join(data_dir, "products.parquet")
    pq.write_table(product_table, parquet_path)
    print(f"Created: {parquet_path}")
    
    # Dataset 3: Multiple partition files (for testing multiple partitions)
    sales_data_q1 = {
        'quarter': ['Q1'] * 4,
        'month': ['Jan', 'Feb', 'Mar', 'Jan'],
        'sales_id': [1001, 1002, 1003, 1004],
        'amount': [15000.0, 18000.0, 22000.0, 16500.0],
        'region': ['North', 'South', 'East', 'West']
    }
    
    sales_data_q2 = {
        'quarter': ['Q2'] * 4,
        'month': ['Apr', 'May', 'Jun', 'Apr'],
        'sales_id': [2001, 2002, 2003, 2004],
        'amount': [19000.0, 21000.0, 25000.0, 18500.0],
        'region': ['North', 'South', 'East', 'West']
    }
    
    sales_data_q3 = {
        'quarter': ['Q3'] * 4,
        'month': ['Jul', 'Aug', 'Sep', 'Jul'],
        'sales_id': [3001, 3002, 3003, 3004],
        'amount': [23000.0, 26000.0, 28000.0, 24000.0],
        'region': ['North', 'South', 'East', 'West']
    }
    
    # Create sales directory for partitioned data
    sales_dir = os.path.join(data_dir, "sales")
    os.makedirs(sales_dir, exist_ok=True)
    
    # Write multiple Arrow files for partition testing
    for quarter_data, quarter_name in [(sales_data_q1, 'Q1'), (sales_data_q2, 'Q2'), (sales_data_q3, 'Q3')]:
        table = pa.table(quarter_data)
        sales_path = os.path.join(sales_dir, f"sales_{quarter_name.lower()}.arrow")
        with pa.ipc.new_file(open(sales_path, 'wb'), table.schema) as writer:
            writer.write_table(table)
        print(f"Created: {sales_path}")
    
    # Create mixed format directory (Arrow + Parquet files)
    mixed_dir = os.path.join(data_dir, "mixed")
    os.makedirs(mixed_dir, exist_ok=True)
    
    # Write one file as Arrow
    customer_data_arrow = {
        'customer_id': [1, 2, 3],
        'name': ['Customer A', 'Customer B', 'Customer C'],
        'email': ['a@example.com', 'b@example.com', 'c@example.com']
    }
    customer_table = pa.table(customer_data_arrow)
    customer_arrow_path = os.path.join(mixed_dir, "customers.arrow")
    with pa.ipc.new_file(open(customer_arrow_path, 'wb'), customer_table.schema) as writer:
        writer.write_table(customer_table)
    print(f"Created: {customer_arrow_path}")
    
    # Write another as Parquet with same schema
    customer_data_parquet = {
        'customer_id': [4, 5, 6],
        'name': ['Customer D', 'Customer E', 'Customer F'],
        'email': ['d@example.com', 'e@example.com', 'f@example.com']
    }
    customer_table_2 = pa.table(customer_data_parquet)
    customer_parquet_path = os.path.join(mixed_dir, "customers_extra.parquet")
    pq.write_table(customer_table_2, customer_parquet_path)
    print(f"Created: {customer_parquet_path}")
    
    print(f"\nTest data files created in: {data_dir}")
    print("Directory structure:")
    for root, dirs, files in os.walk(data_dir):
        level = root.replace(data_dir, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 2 * (level + 1)
        for file in files:
            print(f"{sub_indent}{file}")


if __name__ == "__main__":
    create_test_data_files()
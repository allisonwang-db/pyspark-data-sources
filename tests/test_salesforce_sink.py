#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Salesforce DataSource checkpoint functionality with CSV file streaming (Auto Loader style)
"""
import os
import sys
import shutil
import time
import tempfile
import csv
sys.path.append('.')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_csv_file(file_path, data, headers):
    """Create a CSV file with the given data"""
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        writer.writerows(data)

def test_csv_checkpoint():
    """Test checkpoint functionality using Salesforce streaming write"""
    
    # Check credentials
    username = os.getenv('SALESFORCE_USERNAME')
    password = os.getenv('SALESFORCE_PASSWORD')
    security_token = os.getenv('SALESFORCE_SECURITY_TOKEN')
    
    if not all([username, password, security_token]):
        print("Missing Salesforce credentials in environment variables")
        print("Please set: SALESFORCE_USERNAME, SALESFORCE_PASSWORD, SALESFORCE_SECURITY_TOKEN")
        return False
    
    print("Using Salesforce credentials for user: {}".format(username))
    
    # Create temporary directories
    csv_dir = tempfile.mkdtemp(prefix="test_csv_")
    checkpoint_location = tempfile.mkdtemp(prefix="test_checkpoint_csv_")
    
    print("CSV directory: {}".format(csv_dir))
    print("Checkpoint directory: {}".format(checkpoint_location))
    
    try:
        print("\n=== SETUP: Create Spark Session ===")
        spark = SparkSession.builder \
            .appName("SalesforceCSVCheckpointTest") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        print("Spark session created")
        
        # Register Salesforce DataSource
        from pyspark_datasources.salesforce import SalesforceDataSource
        spark.dataSource.register(SalesforceDataSource)
        print("SalesforceDataSource registered")
        
        # Define schema for our CSV data
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("revenue", DoubleType(), True)
        ])
        
        headers = ["id", "name", "industry", "revenue"]
        
        print("\n=== PHASE 1: Create Initial CSV with 5 Records ===")
        
        # Create initial CSV data
        initial_data = [
            [1, "CSV_Company_A", "Technology", 100000.0],
            [2, "CSV_Company_B", "Finance", 200000.0],
            [3, "CSV_Company_C", "Healthcare", 300000.0],
            [4, "CSV_Company_D", "Manufacturing", 400000.0],
            [5, "CSV_Company_E", "Retail", 500000.0]
        ]
        
        # Create the first CSV file
        initial_csv_path = os.path.join(csv_dir, "batch_001.csv")
        create_csv_file(initial_csv_path, initial_data, headers)
        
        print("Created initial CSV file with 5 records:")
        print("File: {}".format(initial_csv_path))
        
        # Verify the CSV file was created correctly
        test_df = spark.read.format("csv").option("header", "true").schema(schema).load(initial_csv_path)
        print("Initial CSV content:")
        test_df.show()
        
        print("\n=== PHASE 2: First Stream - Read Initial CSV and Write to Salesforce ===")
        
        # Create streaming read from CSV directory
        streaming_df = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(csv_dir)
        
        # Transform for Salesforce Account format
        salesforce_df = streaming_df.select(
            col("name").alias("Name"),
            col("industry").alias("Industry"),
            col("revenue").alias("AnnualRevenue")
        )
        
        print("Starting first stream (should read 5 initial records)...")
        
        # Start first streaming query
        query1 = salesforce_df.writeStream \
            .format("salesforce") \
            .option("username", username) \
            .option("password", password) \
            .option("security_token", security_token) \
            .option("salesforce_object", "Account") \
            .option("batch_size", "10") \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(once=True) \
            .start()
        
        # Wait for completion
        query1.awaitTermination(timeout=60)
        
        # Check for exceptions
        if query1.exception() is not None:
            print("First stream failed with exception: {}".format(query1.exception()))
            return False
        
        # Get progress from first stream
        first_progress = query1.lastProgress
        first_records_processed = 0
        if first_progress:
            print("\nFirst stream progress:")
            print("  - Batch ID: {}".format(first_progress.get('batchId', 'N/A')))
            sources = first_progress.get('sources', [])
            if sources:
                source = sources[0]
                first_records_processed = source.get('numInputRows', 0)
                print("  - Records processed: {}".format(first_records_processed))
                print("  - End offset: {}".format(source.get('endOffset', 'N/A')))
        
        print("First stream completed - processed {} records".format(first_records_processed))
        
        print("\n=== PHASE 3: Add New CSV File with 5 New Records ===")
        
        # Wait a moment to ensure first stream is fully complete
        time.sleep(3)
        
        # Create new CSV data
        new_data = [
            [6, "CSV_Company_F", "Energy", 600000.0],
            [7, "CSV_Company_G", "Education", 700000.0],
            [8, "CSV_Company_H", "Agriculture", 800000.0],
            [9, "CSV_Company_I", "Transportation", 900000.0],
            [10, "CSV_Company_J", "Entertainment", 1000000.0]
        ]
        
        # Create the second CSV file (simulating new data arrival)
        new_csv_path = os.path.join(csv_dir, "batch_002.csv")
        create_csv_file(new_csv_path, new_data, headers)
        
        print("Added new CSV file with 5 records:")
        print("File: {}".format(new_csv_path))
        
        # Verify total files in directory
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        print("CSV files in directory: {}".format(csv_files))
        
        # Verify total records across all files
        all_df = spark.read.format("csv").option("header", "true").schema(schema).load(csv_dir)
        total_records = all_df.count()
        print("Total records across all CSV files: {}".format(total_records))
        
        print("\n=== PHASE 4: Second Stream - Should Only Process New CSV File ===")
        
        # Create new streaming read from the same CSV directory
        streaming_df2 = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(csv_dir)
        
        # Transform for Salesforce (same as before)
        salesforce_df2 = streaming_df2.select(
            col("name").alias("Name"),
            col("industry").alias("Industry"),
            col("revenue").alias("AnnualRevenue")
        )
        
        print("Starting second stream with same checkpoint (should only read new CSV file)...")
        
        # Start second streaming query with SAME checkpoint
        query2 = salesforce_df2.writeStream \
            .format("salesforce") \
            .option("username", username) \
            .option("password", password) \
            .option("security_token", security_token) \
            .option("salesforce_object", "Account") \
            .option("batch_size", "10") \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(once=True) \
            .start()
        
        # Wait for completion
        query2.awaitTermination(timeout=60)
        
        # Check for exceptions
        if query2.exception() is not None:
            print("Second stream failed with exception: {}".format(query2.exception()))
            return False
        
        # Get progress from second stream
        second_progress = query2.lastProgress
        second_records_processed = 0
        if second_progress:
            print("\nSecond stream progress:")
            print("  - Batch ID: {}".format(second_progress.get('batchId', 'N/A')))
            sources = second_progress.get('sources', [])
            if sources:
                source = sources[0]
                second_records_processed = source.get('numInputRows', 0)
                print("  - Records processed: {}".format(second_records_processed))
                print("  - Start offset: {}".format(source.get('startOffset', 'N/A')))
                print("  - End offset: {}".format(source.get('endOffset', 'N/A')))
        
        print("Second stream completed - processed {} records".format(second_records_processed))
        
        print("\n" + "=" * 60)
        print("CHECKPOINT VERIFICATION RESULTS")
        print("=" * 60)
        print("Total CSV files created: {}".format(len(csv_files)))
        print("Total records in all CSV files: {}".format(total_records))
        print("First stream processed:  {} records".format(first_records_processed))
        print("Second stream processed: {} records".format(second_records_processed))
        print("Total records processed: {}".format(first_records_processed + second_records_processed))
        
        # Analyze results - Only one case can be considered true success
        if first_records_processed == 5 and second_records_processed == 5:
            print("\n✅ SUCCESS: Checkpoint functionality working perfectly!")
            print("   - First stream processed initial 5 records from batch_001.csv")
            print("   - Second stream processed only the new 5 records from batch_002.csv")
            print("   - No data was reprocessed (exactly-once semantics)")
            print("   - Total: 10 unique records written to Salesforce")
            return True
        elif first_records_processed == 5 and second_records_processed == 0:
            print("\n⚠️  PARTIAL: Checkpoint prevented reprocessing but no new data detected")
            print("   - First stream processed initial 5 records correctly")
            print("   - Second stream found no new records (timing issue or file not detected)")
            print("   - This shows checkpoint prevents duplicates but doesn't prove full functionality")
            return False
        elif second_records_processed == total_records:
            print("\n❌ CHECKPOINT FAILURE: Second stream reprocessed all data")
            print("   - This indicates checkpoint was not properly restored")
            print("   - Expected: Second stream should only process new files")
            return False
        else:
            print("\n❌ UNEXPECTED RESULT:")
            print("   - First stream: {} records (expected: 5)".format(first_records_processed))
            print("   - Second stream: {} records (expected: 5)".format(second_records_processed))
            print("   - Total available: {} records".format(total_records))
            print("   - Only success case: first=5, second=5 (exactly-once processing)")
            return False
        
    except Exception as e:
        print("Test failed with error: {}".format(str(e)))
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Clean up
        try:
            if 'spark' in locals():
                spark.stop()
            print("\nSpark session stopped")
        except:
            pass
        
        # Clean up directories
        if os.path.exists(csv_dir):
            shutil.rmtree(csv_dir)
            print("Cleaned up CSV directory")
        
        if os.path.exists(checkpoint_location):
            shutil.rmtree(checkpoint_location)
            print("Cleaned up checkpoint directory")

if __name__ == "__main__":
    print("Testing Salesforce DataSource CSV-Based Checkpoint Functionality")
    print("(Similar to Auto Loader pattern)")
    print("=" * 60)
    
    success = test_csv_checkpoint()
    
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    
    if success:
        print("✅ CSV checkpoint test PASSED!")
        print("   The SalesforceDataSource correctly handles checkpoints with file-based streaming")
        print("   This validates exactly-once processing semantics")
    else:
        print("❌ CSV checkpoint test FAILED!")
        print("   Please check the error messages above")
        sys.exit(1)
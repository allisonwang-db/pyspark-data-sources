#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Salesforce Sink Example

This example demonstrates how to use the SalesforceDataSource as a streaming sink
to write data from various sources to Salesforce objects.

Requirements:
- PySpark 4.0+
- simple-salesforce library
- Valid Salesforce credentials

Setup:
    pip install pyspark simple-salesforce

Environment Variables:
    export SALESFORCE_USERNAME="your-username@company.com"
    export SALESFORCE_PASSWORD="your-password"
    export SALESFORCE_SECURITY_TOKEN="your-security-token"
"""

import os
import sys
import tempfile
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def check_credentials():
    """Check if Salesforce credentials are available"""
    username = os.getenv('SALESFORCE_USERNAME')
    password = os.getenv('SALESFORCE_PASSWORD')
    security_token = os.getenv('SALESFORCE_SECURITY_TOKEN')
    
    if not all([username, password, security_token]):
        print("❌ Missing Salesforce credentials!")
        print("Please set the following environment variables:")
        print("  export SALESFORCE_USERNAME='your-username@company.com'")
        print("  export SALESFORCE_PASSWORD='your-password'")
        print("  export SALESFORCE_SECURITY_TOKEN='your-security-token'")
        return False, None, None, None
    
    print(f"✅ Using Salesforce credentials for: {username}")
    return True, username, password, security_token

def example_1_rate_source_to_accounts():
    """Example 1: Stream from rate source to Salesforce Accounts"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Rate Source → Salesforce Accounts")
    print("="*60)
    
    has_creds, username, password, security_token = check_credentials()
    if not has_creds:
        return
    
    spark = SparkSession.builder \
        .appName("SalesforceExample1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        # Register Salesforce sink
        from pyspark_datasources.salesforce import SalesforceDataSource
        spark.dataSource.register(SalesforceDataSource)
        print("✅ Salesforce sink registered")
        
        # Create streaming data from rate source
        streaming_df = spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", 2) \
            .load()
        
        # Transform to Account format
        account_data = streaming_df.select(
            col("timestamp").cast("string").alias("Name"),
            lit("Technology").alias("Industry"),
            (col("value") * 10000).cast("double").alias("AnnualRevenue")
        )
        
        print("📊 Starting streaming write to Salesforce Accounts...")
        
        # Write to Salesforce
        query = account_data.writeStream \
            .format("salesforce-sink") \
            .option("username", username) \
            .option("password", password) \
            .option("security_token", security_token) \
            .option("salesforce_object", "Account") \
            .option("batch_size", "10") \
            .option("checkpointLocation", "/tmp/salesforce_example1_checkpoint") \
            .trigger(once=True) \
            .start()
        
        # Wait for completion
        query.awaitTermination(timeout=60)
        
        # Show results
        progress = query.lastProgress
        if progress:
            sources = progress.get('sources', [])
            if sources:
                records = sources[0].get('numInputRows', 0)
                print(f"✅ Successfully wrote {records} Account records to Salesforce")
            else:
                print("✅ Streaming completed successfully")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        spark.stop()

def example_2_csv_to_contacts():
    """Example 2: Stream from CSV files to Salesforce Contacts"""
    print("\n" + "="*60)
    print("EXAMPLE 2: CSV Files → Salesforce Contacts")
    print("="*60)
    
    has_creds, username, password, security_token = check_credentials()
    if not has_creds:
        return
    
    # Create temporary CSV directory
    csv_dir = tempfile.mkdtemp(prefix="salesforce_contacts_")
    print(f"📁 CSV directory: {csv_dir}")
    
    spark = SparkSession.builder \
        .appName("SalesforceExample2") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        # Register Salesforce sink
        from pyspark_datasources.salesforce import SalesforceDataSource
        spark.dataSource.register(SalesforceDataSource)
        
        # Create sample CSV data
        contact_data = [
            ["John", "Doe", "john.doe@example.com", "555-1234"],
            ["Jane", "Smith", "jane.smith@example.com", "555-5678"],
            ["Bob", "Johnson", "bob.johnson@example.com", "555-9012"]
        ]
        
        headers = ["FirstName", "LastName", "Email", "Phone"]
        csv_file = os.path.join(csv_dir, "contacts.csv")
        
        # Write CSV file
        with open(csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(contact_data)
        
        print(f"📝 Created CSV file with {len(contact_data)} contacts")
        
        # Define schema for CSV
        schema = StructType([
            StructField("FirstName", StringType(), True),
            StructField("LastName", StringType(), True),
            StructField("Email", StringType(), True),
            StructField("Phone", StringType(), True)
        ])
        
        # Stream from CSV
        streaming_df = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(csv_dir)
        
        print("📊 Starting streaming write to Salesforce Contacts...")
        
        # Write to Salesforce with custom schema
        query = streaming_df.writeStream \
            .format("salesforce-sink") \
            .option("username", username) \
            .option("password", password) \
            .option("security_token", security_token) \
            .option("salesforce_object", "Contact") \
            .option("schema", "FirstName STRING, LastName STRING NOT NULL, Email STRING, Phone STRING") \
            .option("batch_size", "5") \
            .option("checkpointLocation", "/tmp/salesforce_example2_checkpoint") \
            .trigger(once=True) \
            .start()
        
        # Wait for completion
        query.awaitTermination(timeout=60)
        
        # Show results
        progress = query.lastProgress
        if progress:
            sources = progress.get('sources', [])
            if sources:
                records = sources[0].get('numInputRows', 0)
                print(f"✅ Successfully wrote {records} Contact records to Salesforce")
            else:
                print("✅ Streaming completed successfully")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        spark.stop()
        # Cleanup
        import shutil
        if os.path.exists(csv_dir):
            shutil.rmtree(csv_dir)

def example_3_checkpoint_demonstration():
    """Example 3: Demonstrate checkpoint functionality with incremental data"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Checkpoint Functionality Demonstration")
    print("="*60)
    
    has_creds, username, password, security_token = check_credentials()
    if not has_creds:
        return
    
    # Create temporary directories
    csv_dir = tempfile.mkdtemp(prefix="salesforce_checkpoint_")
    checkpoint_dir = tempfile.mkdtemp(prefix="checkpoint_")
    
    print(f"📁 CSV directory: {csv_dir}")
    print(f"📁 Checkpoint directory: {checkpoint_dir}")
    
    try:
        # Phase 1: Create initial data and first stream
        print("\n📊 Phase 1: Creating initial batch and first stream...")
        
        initial_data = [
            [1, "InitialCorp_A", "Tech", 100000.0],
            [2, "InitialCorp_B", "Finance", 200000.0],
            [3, "InitialCorp_C", "Healthcare", 300000.0]
        ]
        
        headers = ["id", "name", "industry", "revenue"]
        csv_file1 = os.path.join(csv_dir, "batch_001.csv")
        
        with open(csv_file1, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(initial_data)
        
        # First stream
        spark = SparkSession.builder \
            .appName("SalesforceCheckpointDemo") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        from pyspark_datasources.salesforce import SalesforceDataSource
        spark.dataSource.register(SalesforceDataSource)
        
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("revenue", DoubleType(), True)
        ])
        
        streaming_df1 = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(csv_dir)
        
        account_df1 = streaming_df1.select(
            col("name").alias("Name"),
            col("industry").alias("Industry"),
            col("revenue").alias("AnnualRevenue")
        )
        
        query1 = account_df1.writeStream \
            .format("salesforce-sink") \
            .option("username", username) \
            .option("password", password) \
            .option("security_token", security_token) \
            .option("salesforce_object", "Account") \
            .option("batch_size", "10") \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(once=True) \
            .start()
        
        query1.awaitTermination(timeout=60)
        
        progress1 = query1.lastProgress
        records1 = 0
        if progress1 and progress1.get('sources'):
            records1 = progress1['sources'][0].get('numInputRows', 0)
        
        print(f"   ✅ First stream processed {records1} records")
        
        # Phase 2: Add new data and second stream
        print("\n📊 Phase 2: Adding new batch and second stream...")
        
        import time
        time.sleep(2)  # Brief pause
        
        new_data = [
            [4, "NewCorp_D", "Energy", 400000.0],
            [5, "NewCorp_E", "Retail", 500000.0]
        ]
        
        csv_file2 = os.path.join(csv_dir, "batch_002.csv")
        with open(csv_file2, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(new_data)
        
        # Second stream with same checkpoint
        streaming_df2 = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(csv_dir)
        
        account_df2 = streaming_df2.select(
            col("name").alias("Name"),
            col("industry").alias("Industry"),
            col("revenue").alias("AnnualRevenue")
        )
        
        query2 = account_df2.writeStream \
            .format("salesforce-sink") \
            .option("username", username) \
            .option("password", password) \
            .option("security_token", security_token) \
            .option("salesforce_object", "Account") \
            .option("batch_size", "10") \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(once=True) \
            .start()
        
        query2.awaitTermination(timeout=60)
        
        progress2 = query2.lastProgress
        records2 = 0
        if progress2 and progress2.get('sources'):
            records2 = progress2['sources'][0].get('numInputRows', 0)
        
        print(f"   ✅ Second stream processed {records2} records")
        
        # Analyze checkpoint functionality
        print(f"\n📈 Checkpoint Analysis:")
        print(f"   - First stream:  {records1} records (initial batch)")
        print(f"   - Second stream: {records2} records (new batch)")
        print(f"   - Total:         {records1 + records2} records")
        
        if records1 == 3 and records2 == 2:
            print("   ✅ PERFECT: Exactly-once processing achieved!")
            print("   ✅ Checkpoint functionality working correctly")
        else:
            print("   ⚠️  Results may vary due to timing or file detection")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        spark.stop()
        # Cleanup
        import shutil
        if os.path.exists(csv_dir):
            shutil.rmtree(csv_dir)
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)

def example_4_custom_object():
    """Example 4: Write to custom Salesforce object"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Custom Salesforce Object")
    print("="*60)
    
    has_creds, username, password, security_token = check_credentials()
    if not has_creds:
        return
    
    print("📝 This example shows how to write to custom Salesforce objects")
    print("   Note: Make sure your custom object exists in Salesforce")
    
    spark = SparkSession.builder \
        .appName("SalesforceCustomObjectExample") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    try:
        from pyspark_datasources.salesforce import SalesforceDataSource
        spark.dataSource.register(SalesforceDataSource)
        
        # Create sample data for custom object
        streaming_df = spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", 1) \
            .load()
        
        # Transform for custom object (example: Product__c)
        custom_data = streaming_df.select(
            col("value").cast("string").alias("Product_Code__c"),
            lit("Sample Product").alias("Name"),
            (col("value") * 29.99).cast("double").alias("Price__c"),
            current_timestamp().alias("Created_Date__c")
        )
        
        print("📊 Example configuration for custom object...")
        print("   Custom Object: Product__c")
        print("   Fields: Product_Code__c, Name, Price__c, Created_Date__c")
        print("\n   Note: Uncomment and modify the following code for your custom object:")
        
        # Example code (commented out since custom object may not exist)
        print("""
        query = custom_data.writeStream \\
            .format("salesforce-sink") \\
            .option("username", username) \\
            .option("password", password) \\
            .option("security_token", security_token) \\
            .option("salesforce_object", "Product__c") \\
            .option("schema", "Product_Code__c STRING, Name STRING, Price__c DOUBLE, Created_Date__c TIMESTAMP") \\
            .option("batch_size", "20") \\
            .option("checkpointLocation", "/tmp/custom_object_checkpoint") \\
            .trigger(processingTime="10 seconds") \\
            .start()
        """)
        
        print("✅ Custom object example configuration shown")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        spark.stop()

def main():
    """Run all examples"""
    print("🚀 Salesforce Sink Examples")
    print("This demonstrates various ways to use the Salesforce streaming sink")
    
    try:
        # Run examples
        example_1_rate_source_to_accounts()
        example_2_csv_to_contacts()
        example_3_checkpoint_demonstration()
        example_4_custom_object()
        
        print("\n" + "="*60)
        print("✅ All examples completed!")
        print("="*60)
        print("\n💡 Key takeaways:")
        print("   - Salesforce sink supports various input sources (rate, CSV, etc.)")
        print("   - Checkpoint functionality enables exactly-once processing")
        print("   - Custom schemas allow flexibility for different Salesforce objects")
        print("   - Batch processing optimizes Salesforce API usage")
        print("   - Error handling provides fallback to individual record creation")
        
    except KeyboardInterrupt:
        print("\n⏹️  Examples interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")

if __name__ == "__main__":
    main()
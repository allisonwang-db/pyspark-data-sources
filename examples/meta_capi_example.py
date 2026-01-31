#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Meta Conversions API (CAPI) Datasource Example

This example demonstrates how to use the MetaCapiDataSource as a datasource
to write event data to Meta for ad optimization.

Requirements:
- PySpark
- requests
- Valid Meta System User Access Token and Pixel ID

Setup:
    pip install pyspark requests

Environment Variables:
    export META_ACCESS_TOKEN="your-access-token"
    export META_PIXEL_ID="your-pixel-id"
"""

import os
import tempfile
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


def check_credentials():
    """Check if Meta credentials are available"""
    token = os.getenv("META_ACCESS_TOKEN")
    pixel_id = os.getenv("META_PIXEL_ID")

    if not all([token, pixel_id]):
        print("❌ Missing Meta credentials!")
        print("Please set the following environment variables:")
        print("  export META_ACCESS_TOKEN='your-access-token'")
        print("  export META_PIXEL_ID='your-pixel-id'")
        return False, None, None

    print(f"✅ Using Pixel ID: {pixel_id}")
    return True, token, pixel_id


def example_1_rate_source_to_capi():
    """Example 1: Stream simulated purchases to Meta CAPI"""
    print("\n" + "=" * 60)
    print("EXAMPLE 1: Simulated Purchases → Meta CAPI (Streaming)")
    print("=" * 60)

    has_creds, token, pixel_id = check_credentials()
    if not has_creds:
        return

    spark = SparkSession.builder.appName("MetaCapiExample1").getOrCreate()

    try:
        from pyspark_datasources.meta_capi import MetaCapiDataSource

        spark.dataSource.register(MetaCapiDataSource)
        print("✅ Meta CAPI datasource registered")

        # Create streaming data (simulating 1 purchase per second)
        streaming_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

        # Transform to CAPI format (Flat Mode)
        # We simulate user data. In production, this comes from your tables.
        events_df = streaming_df.select(
            lit("Purchase").alias("event_name"),
            col("timestamp").alias("event_time"),
            lit("test@example.com").alias("email"),  # Will be auto-hashed
            lit("website").alias("action_source"),
            (col("value") * 10.0 + 5.0).alias("value"),
            lit("USD").alias("currency"),
            lit("TEST12345").alias("test_event_code"),  # For testing in Events Manager
        )

        print("📊 Starting streaming write to Meta CAPI...")
        print("   Check your Events Manager 'Test Events' tab!")

        # Write to Meta CAPI
        query = (
            events_df.writeStream.format("meta_capi")
            .option("access_token", token)
            .option("pixel_id", pixel_id)
            .option("test_event_code", "TEST12345")  # Optional: direct test code option
            .option("batch_size", "10")
            .option("checkpointLocation", "/tmp/meta_capi_example1_checkpoint")
            .trigger(processingTime="10 seconds")
            .start()
        )

        # Run for 30 seconds then stop
        time.sleep(30)
        query.stop()
        print("✅ Streaming stopped")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        spark.stop()


def example_2_batch_dataframe_to_capi():
    """Example 2: Batch write a static DataFrame to Meta CAPI"""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Static DataFrame → Meta CAPI (Batch)")
    print("=" * 60)

    has_creds, token, pixel_id = check_credentials()
    if not has_creds:
        return

    spark = SparkSession.builder.appName("MetaCapiExample2").getOrCreate()

    try:
        from pyspark_datasources.meta_capi import MetaCapiDataSource

        spark.dataSource.register(MetaCapiDataSource)
        print("✅ Meta CAPI datasource registered")

        # Create sample data
        data = [
            ("Purchase", 1700000001, "user1@example.com", 120.50, "USD"),
            ("Purchase", 1700000002, "user2@example.com", 85.00, "USD"),
            ("AddToCart", 1700000003, "user3@example.com", 25.99, "USD"),
        ]

        columns = ["event_name", "event_time", "email", "value", "currency"]
        df = spark.createDataFrame(data, columns)

        # Add optional fields
        df = df.withColumn("action_source", lit("website")).withColumn(
            "test_event_code", lit("TEST12345")
        )

        print(f"📊 Writing {df.count()} records to Meta CAPI in batch mode...")
        print("   Check your Events Manager 'Test Events' tab!")

        # Write to Meta CAPI (Batch)
        df.write.format("meta_capi").option("access_token", token).option(
            "pixel_id", pixel_id
        ).option("test_event_code", "TEST12345").option("batch_size", "50").save()

        print("✅ Batch write completed")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        spark.stop()


def main():
    print("🚀 Meta CAPI Datasource Example")
    example_1_rate_source_to_capi()
    example_2_batch_dataframe_to_capi()


if __name__ == "__main__":
    main()

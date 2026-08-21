#!/usr/bin/env python3
"""
SharePoint / Teams Excel Reader — Example Script

Demonstrates how to use SharePointExcelDataSource to read Excel files stored in
SharePoint Online or Microsoft Teams into a Spark DataFrame.

Requirements:
    pip install pyspark-data-sources[sharepoint-excel]

Environment Variables:
    export SHAREPOINT_TENANT_ID="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    export SHAREPOINT_CLIENT_ID="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    export SHAREPOINT_CLIENT_SECRET="your-client-secret"
    export SHAREPOINT_SITE_HOST="contoso.sharepoint.com"
    export SHAREPOINT_SITE_PATH="/sites/MySharePoint"
    export SHAREPOINT_FILE_PATH="/General/data.xlsx"
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


def _load_config() -> dict:
    """Load SharePoint connection config from environment variables."""
    config = {
        "tenant_id": os.getenv("SHAREPOINT_TENANT_ID"),
        "client_id": os.getenv("SHAREPOINT_CLIENT_ID"),
        "client_secret": os.getenv("SHAREPOINT_CLIENT_SECRET"),
        "site_host": os.getenv("SHAREPOINT_SITE_HOST"),
        "site_path": os.getenv("SHAREPOINT_SITE_PATH"),
        "file_path": os.getenv("SHAREPOINT_FILE_PATH"),
    }
    missing = [k for k, v in config.items() if not v]
    if missing:
        print("Missing required environment variables:")
        for key in missing:
            print(f"  export {key.upper().replace('.', '_')}='<value>'")
        print("\nSet them and re-run.")
        sys.exit(1)
    return config


def _spark() -> SparkSession:
    return SparkSession.builder.appName("sharepoint-excel-example").getOrCreate()


# ---------------------------------------------------------------------------
# Example 1 — Basic read, default all-string schema
# ---------------------------------------------------------------------------

def example_basic_read(config: dict) -> None:
    print("\n" + "=" * 60)
    print("EXAMPLE 1: Basic read — default all-string schema")
    print("=" * 60)

    from pyspark_datasources import SharePointExcelDataSource

    spark = _spark()
    spark.dataSource.register(SharePointExcelDataSource)

    df = (
        spark.read.format("sharepoint_excel")
        .option("tenant_id",    config["tenant_id"])
        .option("client_id",    config["client_id"])
        .option("client_secret",config["client_secret"])
        .option("site_host",    config["site_host"])
        .option("site_path",    config["site_path"])
        .option("file_path",    config["file_path"])
        .load()
    )

    print(f"Row count : {df.count()}")
    print(f"Columns   : {df.columns}")
    df.printSchema()
    df.show(10, truncate=False)

    spark.stop()


# ---------------------------------------------------------------------------
# Example 2 — Read a specific sheet with a row limit
# ---------------------------------------------------------------------------

def example_specific_sheet(config: dict) -> None:
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Read specific sheet with row limit")
    print("=" * 60)

    sheet_name = os.getenv("SHAREPOINT_SHEET_NAME", "Sheet1")
    nrows = os.getenv("SHAREPOINT_NROWS", "100")

    from pyspark_datasources import SharePointExcelDataSource

    spark = _spark()
    spark.dataSource.register(SharePointExcelDataSource)

    df = (
        spark.read.format("sharepoint_excel")
        .option("tenant_id",    config["tenant_id"])
        .option("client_id",    config["client_id"])
        .option("client_secret",config["client_secret"])
        .option("site_host",    config["site_host"])
        .option("site_path",    config["site_path"])
        .option("file_path",    config["file_path"])
        .option("sheet_name",   sheet_name)
        .option("nrows",        nrows)
        .load()
    )

    print(f"Sheet     : {sheet_name}")
    print(f"Rows read : {df.count()} (limit {nrows})")
    df.show(5, truncate=False)

    spark.stop()


# ---------------------------------------------------------------------------
# Example 3 — Teams channel file
# ---------------------------------------------------------------------------

def example_teams_channel(config: dict) -> None:
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Read from a Microsoft Teams channel")
    print("=" * 60)

    # Teams-backed SharePoint uses /teams/<channel-name> as the site path
    teams_site_path = os.getenv("SHAREPOINT_TEAMS_SITE_PATH", "/teams/DataTeam")
    teams_file_path = os.getenv("SHAREPOINT_TEAMS_FILE_PATH", "/General/report.xlsx")

    from pyspark_datasources import SharePointExcelDataSource

    spark = _spark()
    spark.dataSource.register(SharePointExcelDataSource)

    df = (
        spark.read.format("sharepoint_excel")
        .option("tenant_id",    config["tenant_id"])
        .option("client_id",    config["client_id"])
        .option("client_secret",config["client_secret"])
        .option("site_host",    config["site_host"])
        .option("site_path",    teams_site_path)
        .option("file_path",    teams_file_path)
        .load()
    )

    print(f"Teams path: {config['site_host']}{teams_site_path}{teams_file_path}")
    print(f"Rows      : {df.count()}")
    df.show(5, truncate=False)

    spark.stop()


# ---------------------------------------------------------------------------
# Example 4 — Custom typed schema
# ---------------------------------------------------------------------------

def example_custom_schema(config: dict) -> None:
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Custom typed schema")
    print("=" * 60)

    # Adjust to match your actual Excel column names and types
    schema = StructType([
        StructField("name",     StringType()),
        StructField("quantity", LongType()),
        StructField("revenue",  DoubleType()),
    ])

    from pyspark_datasources import SharePointExcelDataSource

    spark = _spark()
    spark.dataSource.register(SharePointExcelDataSource)

    df = (
        spark.read.format("sharepoint_excel")
        .schema(schema)
        .option("tenant_id",    config["tenant_id"])
        .option("client_id",    config["client_id"])
        .option("client_secret",config["client_secret"])
        .option("site_host",    config["site_host"])
        .option("site_path",    config["site_path"])
        .option("file_path",    config["file_path"])
        .load()
    )

    df.printSchema()
    df.show(10)

    total_revenue = df.agg({"revenue": "sum"}).collect()[0][0]
    print(f"Total revenue: {total_revenue}")

    spark.stop()


# ---------------------------------------------------------------------------
# Example 5 — Write result to Parquet
# ---------------------------------------------------------------------------

def example_write_parquet(config: dict) -> None:
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Read from SharePoint and write to Parquet")
    print("=" * 60)

    output_path = os.getenv("OUTPUT_PARQUET_PATH", "/tmp/sharepoint_excel_output")

    from pyspark_datasources import SharePointExcelDataSource

    spark = _spark()
    spark.dataSource.register(SharePointExcelDataSource)

    df = (
        spark.read.format("sharepoint_excel")
        .option("tenant_id",    config["tenant_id"])
        .option("client_id",    config["client_id"])
        .option("client_secret",config["client_secret"])
        .option("site_host",    config["site_host"])
        .option("site_path",    config["site_path"])
        .option("file_path",    config["file_path"])
        .load()
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"Written {df.count()} rows to Parquet: {output_path}")

    # Verify round-trip
    readback = spark.read.parquet(output_path)
    print(f"Parquet row count: {readback.count()}")

    spark.stop()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("SharePoint / Teams Excel Reader — Examples")
    print("Reads Excel files from SharePoint into a Spark DataFrame.\n")

    config = _load_config()

    try:
        example_basic_read(config)
        example_specific_sheet(config)
        example_teams_channel(config)
        example_custom_schema(config)
        example_write_parquet(config)

        print("\n" + "=" * 60)
        print("All examples completed successfully.")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as exc:
        print(f"\nError: {exc}")
        raise


if __name__ == "__main__":
    main()

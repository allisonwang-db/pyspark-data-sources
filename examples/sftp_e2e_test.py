"""
End-to-End Test for SFTP Data Source

This script demonstrates how to run an end-to-end test for the SFTP data source
using a local SFTP server running in Docker.

Prerequisites:
1.  Docker installed and running.
2.  `paramiko` library installed (`pip install paramiko`).

Usage:
1.  Start a local SFTP server using Docker:
    ```bash
    docker run -p 2222:22 -d atmoz/sftp foo:pass:::upload
    ```

2.  Run this script:
    ```bash
    python examples/sftp_e2e_test.py
    ```

The script performs the following steps:
1.  Connects to the SFTP server using `paramiko` and uploads test data.
2.  Runs a Spark job to read the data using the `sftp` data source.
3.  Runs a Spark job to write the data back to a new directory on the SFTP server.
4.  Verifies the output files using `paramiko`.
"""
import os
import time
import pytest
from pyspark.sql import SparkSession

# Note: This test requires a running SFTP server.
# You can run one locally using Docker:
# docker run -p 2222:22 -d atmoz/sftp foo:pass:::upload

def test_sftp_e2e():
    try:
        import paramiko
    except ImportError:
        pytest.skip("paramiko not installed")

    # SFTP Connection Details
    HOST = "localhost"
    PORT = 2222
    USERNAME = "foo"
    PASSWORD = "pass"
    UPLOAD_DIR = "/upload"
    
    # 1. Setup: Use paramiko to upload test data
    transport = paramiko.Transport((HOST, PORT))
    try:
        transport.connect(username=USERNAME, password=PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        # Create a test file
        input_file = f"{UPLOAD_DIR}/input.txt"
        with sftp.open(input_file, "w") as f:
            f.write("line1\nline2\nline3")
            
        print(f"Uploaded test data to {input_file}")
        
    except Exception as e:
        pytest.skip(f"Could not connect to SFTP server: {e}")
        return
    finally:
        if 'sftp' in locals(): sftp.close()
        if 'transport' in locals(): transport.close()

    # 2. Run Spark Job: Read from SFTP
    spark = SparkSession.builder \
        .appName("SFTP E2E Test") \
        .master("local[*]") \
        .getOrCreate()
        
    # Register the data source
    from pyspark_datasources import SFTPDataSource
    spark.dataSource.register(SFTPDataSource)
    
    try:
        print("Reading from SFTP...")
        df = spark.read.format("sftp") \
            .option("host", HOST) \
            .option("port", PORT) \
            .option("username", USERNAME) \
            .option("password", PASSWORD) \
            .option("path", input_file) \
            .load()
            
        rows = df.collect()
        print(f"Read {len(rows)} rows")
        assert len(rows) == 3
        assert rows[0].value == "line1"
        
        # 3. Run Spark Job: Write to SFTP
        output_dir = f"{UPLOAD_DIR}/output_{int(time.time())}"
        print(f"Writing to {output_dir}...")
        
        df.write.format("sftp") \
            .option("host", HOST) \
            .option("port", PORT) \
            .option("username", USERNAME) \
            .option("password", PASSWORD) \
            .option("path", output_dir) \
            .save()
            
        # 4. Verify: Use paramiko to check output files
        transport = paramiko.Transport((HOST, PORT))
        transport.connect(username=USERNAME, password=PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        files = sftp.listdir(output_dir)
        data_files = [f for f in files if f.startswith("part-")]
        assert len(data_files) > 0
        
        with sftp.open(f"{output_dir}/{data_files[0]}", "r") as f:
            content = f.read().decode('utf-8')
            print("Output content:", content)
            assert "line1" in content
            
    finally:
        spark.stop()
        if 'sftp' in locals(): sftp.close()
        if 'transport' in locals(): transport.close()

if __name__ == "__main__":
    test_sftp_e2e()

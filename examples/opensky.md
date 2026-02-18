# OpenSky Network Data Source Example

Stream real-time flight tracking data. Anonymous access has low rate limits; OAuth for higher limits.

## Setup Credentials

For **anonymous access**: No credentials. Limited to ~100 API calls/day.

For **authenticated access** (higher limits), set these variables. For anonymous access, use empty strings:

```python
OPENSKY_CLIENT_ID = ""  # or "your-client-id"
OPENSKY_CLIENT_SECRET = ""  # or "your-client-secret"
```

Request API access at: https://opensky-network.org/apply-for-api-access (academic/research).

## End-to-End Pipeline: Streaming Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import OpenSkyDataSource

spark = SparkSession.builder.appName("opensky-stream").getOrCreate()
spark.dataSource.register(OpenSkyDataSource)
```

### Step 2: Stream Flight Data (with optional auth)

```python
stream = (
    spark.readStream.format("opensky")
    .option("region", "EUROPE")
    .option("client_id", OPENSKY_CLIENT_ID)
    .option("client_secret", OPENSKY_CLIENT_SECRET)
    .load()
)

query = stream.writeStream.format("console").outputMode("append").trigger(once=True).start()
query.awaitTermination()
```

### Example Output

```
+------+--------+--------------+--------+---------+--------+
|icao24|callsign|origin_country|latitude|longitude|altitude|
+------+--------+--------------+--------+---------+--------+
|4b1806|SWR123  |Switzerland   |47.3765 |8.5123   |10058.4 |
|3c6750|DLH456  |Germany       |52.5200 |13.4050  |11887.2 |
+------+--------+--------------+--------+---------+--------+
```

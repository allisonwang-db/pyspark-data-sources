# Weather Data Source Example

Fetch weather data from tomorrow.io. Streaming read only. Requires API key.

## Setup Credentials

```python
TOMORROW_IO_API_KEY = "your-api-key"
```

Get API key at: https://www.tomorrow.io/weather-api/

## End-to-End Pipeline: Streaming Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import WeatherDataSource

spark = SparkSession.builder.appName("weather-example").getOrCreate()
spark.dataSource.register(WeatherDataSource)
```

### Step 2: Stream Weather Data for Locations

```python
# Locations as list of (lat, lon) tuples
options = {
    "locations": "[(37.7749, -122.4194), (40.7128, -74.0060)]",  # SF, NYC
    "apikey": TOMORROW_IO_API_KEY,
    "frequency": "minutely",  # or "hourly", "daily"
}

weather_df = spark.readStream.format("weather").options(**options).load()
```

### Step 3: Process the Stream

```python
query = weather_df.writeStream.format("console").outputMode("append").trigger(availableNow=True).start()
query.awaitTermination()
```

### Example Output

```
+--------+---------+--------+----------+
|latitude|longitude|weather |timestamp |
+--------+---------+--------+----------+
|37.7749 |-122.4194|Clear  |2024-01-15|
|40.7128 |-74.0060 |Cloudy |2024-01-15|
+--------+---------+--------+----------+
```

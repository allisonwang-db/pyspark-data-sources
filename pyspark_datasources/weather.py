import ast
import json
import requests

from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.types import StructType, StructField, DoubleType, StringType


class WeatherDataSource(DataSource):
    """
    A custom PySpark data source for fetching weather data from tomorrow.io for given 
    locations (latitude, longitude).

    Options
    -------

    - locations: specify a list of (latitude, longitude) tuples.
    - apikey: specify the API key for the weather service (tomorrow.io).
    - frequency: specify the frequency of the data ("minutely", "hourly", "daily"). 
                 Default is "minutely".

    Examples
    --------

    Register the data source.

    >>> from pyspark_datasources import WeatherDataSource
    >>> spark.dataSource.register(WeatherDataSource)


    Define the options for the custom data source
    
    >>> options = {
    ...    "locations": "[(37.7749, -122.4194), (40.7128, -74.0060)]",  # San Francisco and New York
    ...    "apikey": "your_api_key_here",
    ... }

    Create a DataFrame using the custom weather data source
    
    >>> weather_df = spark.readStream.format("weather").options(**options).load()

    Stream weather data and print the results to the console in real-time.

    >>> query = weather_df.writeStream.format("console").trigger(availableNow=True).start()
    """

    @classmethod
    def name(cls):
        """Returns the name of the data source."""
        return "weather"

    def __init__(self, options):
        """Initialize with options provided."""
        self.options = options
        self.frequency = options.get("frequency", "minutely")
        if self.frequency not in ["minutely", "hourly", "daily"]:
            raise ValueError(f"Unsupported frequency: {self.frequency}")

    def schema(self):
        """Defines the output schema of the data source."""
        return StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("weather", StringType(), True),
            StructField("timestamp", StringType(), True),
        ])

    def simpleStreamReader(self, schema: StructType):
        """Returns an instance of the reader for this data source."""
        return WeatherSimpleStreamReader(schema, self.options)


class WeatherSimpleStreamReader(SimpleDataSourceStreamReader):

    def initialOffset(self):
        """
        Returns the initial offset for reading, which serves as the starting point for 
        the streaming data source.

        The initial offset is returned as a dictionary where each key is a unique identifier
        for a specific (latitude, longitude) pair, and each value is a timestamp string 
        (in ISO 8601 format) representing the point in time from which data should start being 
        read.

        Example:
            For locations [(37.7749, -122.4194), (40.7128, -74.0060)], the offset might look like:
            {
                "offset_37.7749_-122.4194": "2024-09-01T00:00:00Z",
                "offset_40.7128_-74.0060": "2024-09-01T00:00:00Z"
            }
        """
        return {f"offset_{lat}_{long}": "2024-09-01T00:00:00Z" for (lat, long) in self.locations}
    
    @staticmethod
    def _parse_locations(locations_str: str):
        """Converts string representation of list of tuples to actual list of tuples."""
        return [tuple(map(float, x)) for x in ast.literal_eval(locations_str)]

    def __init__(self, schema: StructType, options: dict):
        """Initialize with schema and options."""
        super().__init__()
        self.schema = schema
        self.locations = self._parse_locations(options.get("locations", "[]"))
        self.api_key = options.get("apikey", "")
        self.current = 0
        self.frequency = options.get("frequency", "minutely")
        self.session = requests.Session()  # Use a session for connection pooling

    def read(self, start: dict):
        """Reads data starting from the given offset."""
        data = []
        new_offset = {}
        for lat, long in self.locations:
            start_ts = start[f"offset_{lat}_{long}"]
            weather = self._fetch_weather(lat, long, self.api_key, self.session)[self.frequency]
            for entry in weather:
                # Start time is exclusive and end time is inclusive.
                if entry["time"] > start_ts:
                    data.append((lat, long, json.dumps(entry["values"]), entry["time"]))
            new_offset.update({f"offset_{lat}_{long}": weather[-1]["time"]})
        return (data, new_offset)

    @staticmethod
    def _fetch_weather(lat: float, long: float, api_key: str, session):
        """Fetches weather data for the given latitude and longitude using a REST API."""
        url = f"https://api.tomorrow.io/v4/weather/forecast?location={lat},{long}&apikey={api_key}"
        response = session.get(url)
        response.raise_for_status()
        return response.json()["timelines"]

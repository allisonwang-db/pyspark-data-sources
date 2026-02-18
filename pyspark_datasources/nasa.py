"""NASA API data source - reads APOD and other NASA data."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class NasaDataSource(DataSource):
    """
    A DataSource for reading data from the NASA API.

    Requires a free API key from https://api.nasa.gov/.

    Name: `nasa`

    Schema: `date string, title string, explanation string, url string,
    hdurl string, media_type string, copyright string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import NasaDataSource
    >>> spark.dataSource.register(NasaDataSource)

    Load Astronomy Picture of the Day entries.

    >>> spark.read.format("nasa").option("api_key", "YOUR_KEY").load().show()

    Load 10 APOD entries.

    >>> spark.read.format("nasa").option("api_key", "YOUR_KEY").option("count", "10").load()
    """

    @classmethod
    def name(cls):
        return "nasa"

    def schema(self):
        return (
            "date string, title string, explanation string, url string, "
            "hdurl string, media_type string, copyright string"
        )

    def reader(self, schema):
        return NasaReader(self.options)


class NasaReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.api_key = self.options.get("api_key")
        if not self.api_key:
            raise ValueError("api_key option is required for NasaDataSource")
        self.count = int(self.options.get("count", "5"))
        self.count = min(max(self.count, 1), 100)

    def read(self, partition):
        url = "https://api.nasa.gov/planetary/apod"
        try:
            response = requests.get(
                url,
                params={"api_key": self.api_key, "count": self.count},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and "error" in data:
                return iter([])

            items = data if isinstance(data, list) else [data]

            for item in items:
                yield Row(
                    date=item.get("date", ""),
                    title=item.get("title", ""),
                    explanation=(item.get("explanation") or "")[:500],
                    url=item.get("url", ""),
                    hdurl=item.get("hdurl", ""),
                    media_type=item.get("media_type", ""),
                    copyright=item.get("copyright", ""),
                )
        except requests.RequestException:
            return iter([])

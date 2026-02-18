"""Lorem Picsum API data source - placeholder photos with metadata."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class PicsumDataSource(DataSource):
    """
    A DataSource for reading photo metadata from picsum.photos.
    No API key. Name: `picsum`
    Schema: `id string, author string, width int, height int, url string`
    """

    @classmethod
    def name(cls):
        return "picsum"

    def schema(self):
        return "id string, author string, width int, height int, url string"

    def reader(self, schema):
        return PicsumReader(self.options)


class PicsumReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.limit = min(int(options.get("limit", "30")), 100)

    def read(self, partition):
        try:
            response = requests.get(
                "https://picsum.photos/v2/list",
                params={"page": 1, "limit": self.limit},
                timeout=30,
            )
            response.raise_for_status()
            for p in response.json():
                yield Row(
                    id=p.get("id", ""),
                    author=p.get("author", ""),
                    width=p.get("width", 0),
                    height=p.get("height", 0),
                    url=p.get("url", ""),
                )
        except requests.RequestException:
            return iter([])

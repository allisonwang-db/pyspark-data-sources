"""Quotable API data source - reads inspirational quotes."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class QuotableDataSource(DataSource):
    """
    A DataSource for reading quotes from the Quotable API.
    No API key required. Name: `quotable`
    Schema: `id string, content string, author string, tags string`
    """

    @classmethod
    def name(cls):
        return "quotable"

    def schema(self):
        return "id string, content string, author string, tags string"

    def reader(self, schema):
        return QuotableReader(self.options)


class QuotableReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.limit = min(int(options.get("limit", "20")), 150)

    def read(self, partition):
        try:
            response = requests.get(
                "https://api.quotable.io/quotes",
                params={"limit": self.limit},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            for q in data.get("results", []):
                tags = ",".join(q.get("tags", []))
                yield Row(
                    id=q.get("_id", ""),
                    content=(q.get("content", ""))[:1000],
                    author=q.get("author", ""),
                    tags=tags,
                )
        except requests.RequestException:
            return iter([])

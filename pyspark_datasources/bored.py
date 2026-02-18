"""Bored API data source - reads random activity suggestions."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class BoredDataSource(DataSource):
    """
    A DataSource for reading activity suggestions from the Bored API.
    No API key required. Name: `bored`
    Schema: `activity string, type string, participants int, price double, key string`
    """

    @classmethod
    def name(cls):
        return "bored"

    def schema(self):
        return "activity string, type string, participants int, price double, key string"

    def reader(self, schema):
        return BoredReader(self.options)


class BoredReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.count = min(int(options.get("limit", "10")), 50)

    def read(self, partition):
        try:
            rows = []
            seen = set()
            for _ in range(self.count):
                response = requests.get("https://www.boredapi.com/api/activity", timeout=10)
                response.raise_for_status()
                a = response.json()
                if "error" in a:
                    break
                key = a.get("key", "")
                if key not in seen:
                    seen.add(key)
                    rows.append(
                        Row(
                            activity=a.get("activity", ""),
                            type=a.get("type", ""),
                            participants=a.get("participants", 0),
                            price=float(a.get("price", 0)),
                            key=key,
                        )
                    )
            return iter(rows)
        except requests.RequestException:
            return iter([])

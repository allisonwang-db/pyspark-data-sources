"""Agify API data source - age prediction from first name."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class AgifyDataSource(DataSource):
    """
    A DataSource for age prediction from api.agify.io.
    No API key. Path = name. Name: `agify`
    Schema: `name string, age int, count long`
    """

    @classmethod
    def name(cls):
        return "agify"

    def schema(self):
        return "name string, age int, count long"

    def reader(self, schema):
        return AgifyReader(self.options)


class AgifyReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.name_param = options.get("path") or "Michael"

    def read(self, partition):
        try:
            response = requests.get(
                "https://api.agify.io/",
                params={"name": self.name_param},
                timeout=10,
            )
            response.raise_for_status()
            d = response.json()
            yield Row(
                name=d.get("name", ""),
                age=d.get("age") or 0,
                count=d.get("count") or 0,
            )
        except requests.RequestException:
            return iter([])

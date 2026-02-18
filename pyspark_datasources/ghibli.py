"""Studio Ghibli API data source - reads films from Ghibli API."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class GhibliDataSource(DataSource):
    """
    A DataSource for reading Studio Ghibli films from ghibliapi.vercel.app.
    No API key required. Name: `ghibli`
    Schema: `id string, title string, director string, release_date string, rt_score string`
    """

    @classmethod
    def name(cls):
        return "ghibli"

    def schema(self):
        return "id string, title string, director string, release_date string, rt_score string"

    def reader(self, schema):
        return GhibliReader(self.options)


class GhibliReader(DataSourceReader):
    def __init__(self, options):
        self.options = options

    def read(self, partition):
        try:
            response = requests.get("https://ghibliapi.vercel.app/films", timeout=30)
            response.raise_for_status()
            for f in response.json():
                yield Row(
                    id=f.get("id", ""),
                    title=f.get("title", ""),
                    director=f.get("director", ""),
                    release_date=f.get("release_date", ""),
                    rt_score=f.get("rt_score", ""),
                )
        except requests.RequestException:
            return iter([])

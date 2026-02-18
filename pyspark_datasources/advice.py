"""Advice Slip API data source - random advice."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class AdviceDataSource(DataSource):
    """
    A DataSource for reading random advice from api.adviceslip.com.
    No API key. Name: `advice`
    Schema: `id string, advice string`
    """

    @classmethod
    def name(cls):
        return "advice"

    def schema(self):
        return "id string, advice string"

    def reader(self, schema):
        return AdviceReader(self.options)


class AdviceReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.count = min(int(options.get("limit", "5")), 30)

    def read(self, partition):
        try:
            rows = []
            seen = set()
            for _ in range(self.count):
                r = requests.get("https://api.adviceslip.com/advice", timeout=10)
                r.raise_for_status()
                data = r.json()
                slip = data.get("slip", {})
                sid = slip.get("id", "")
                if sid not in seen:
                    seen.add(sid)
                    rows.append(Row(id=str(sid), advice=slip.get("advice", "")))
            return iter(rows)
        except requests.RequestException:
            return iter([])

"""HTTPBin data source - reads test data from httpbin.org."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class HttpbinDataSource(DataSource):
    """
    A DataSource for reading test data from httpbin.org.
    No API key required. Name: `httpbin`
    Schema: `origin string, url string, args string, headers string`
    """

    @classmethod
    def name(cls):
        return "httpbin"

    def schema(self):
        return "origin string, url string, args string, headers string"

    def reader(self, schema):
        return HttpbinReader(self.options)


class HttpbinReader(DataSourceReader):
    def __init__(self, options):
        self.options = options

    def read(self, partition):
        try:
            response = requests.get("https://httpbin.org/get", timeout=10)
            response.raise_for_status()
            data = response.json()
            args_str = str(data.get("args", {}))[:500]
            headers_str = str(dict(list(data.get("headers", {}).items())[:5]))[:500]
            yield Row(
                origin=data.get("origin", ""),
                url=data.get("url", ""),
                args=args_str,
                headers=headers_str,
            )
        except requests.RequestException:
            return iter([])

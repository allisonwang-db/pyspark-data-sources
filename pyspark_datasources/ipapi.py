"""IP-API data source - geolocation by IP address."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class IpApiDataSource(DataSource):
    """
    A DataSource for IP geolocation from ip-api.com.
    No API key. Path = IP (default: your IP). Name: `ipapi`
    Schema: `query string, country string, city string, isp string, org string`
    """

    @classmethod
    def name(cls):
        return "ipapi"

    def schema(self):
        return "query string, country string, city string, isp string, org string"

    def reader(self, schema):
        return IpApiReader(self.options)


class IpApiReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.ip = options.get("path") or options.get("ip", "")

    def read(self, partition):
        try:
            url = "http://ip-api.com/json/" + (self.ip if self.ip else "")
            if url.endswith("/"):
                url = "http://ip-api.com/json"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            d = response.json()
            if d.get("status") == "fail":
                return iter([])
            yield Row(
                query=d.get("query", ""),
                country=d.get("country", ""),
                city=d.get("city", ""),
                isp=d.get("isp", ""),
                org=d.get("org", ""),
            )
        except requests.RequestException:
            return iter([])

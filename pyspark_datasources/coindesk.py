"""CoinDesk API data source - Bitcoin price."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class CoinDeskDataSource(DataSource):
    """
    A DataSource for reading Bitcoin price from api.coindesk.com.
    No API key. Name: `coindesk`
    Schema: `code string, rate string, rate_float double, updated string`
    """

    @classmethod
    def name(cls):
        return "coindesk"

    def schema(self):
        return "code string, rate string, rate_float double, updated string"

    def reader(self, schema):
        return CoinDeskReader(self.options)


class CoinDeskReader(DataSourceReader):
    def __init__(self, options):
        self.options = options

    def read(self, partition):
        try:
            response = requests.get(
                "https://api.coindesk.com/v1/bpi/currentprice/USD.json",
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            bpi = data.get("bpi", {}).get("USD", {})
            time = data.get("time", {}).get("updatedISO", "")
            yield Row(
                code=bpi.get("code", "USD"),
                rate=bpi.get("rate", ""),
                rate_float=float(bpi.get("rate_float", 0)),
                updated=time,
            )
        except requests.RequestException:
            return iter([])

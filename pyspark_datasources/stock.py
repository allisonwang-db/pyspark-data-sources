from dataclasses import dataclass
from typing import Dict

from pyspark.sql.types import StructType
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition


class StockDataSource(DataSource):
    """
    A data source for reading stock data using the Alpha Vantage API.

    Examples
    --------

    Load the daily stock data for SPY:

    >>> df = spark.read.format("stock").option("api_key", "your-key").load("SPY")
    >>> df.show(n=5)
    +----------+------+------+------+------+--------+------+
    |      date|  open|  high|   low| close|  volume|symbol|
    +----------+------+------+------+------+--------+------+
    |2024-06-04|526.46|529.15|524.96|528.39|33898396|   SPY|
    |2024-06-03|529.02|529.31| 522.6| 527.8|46835702|   SPY|
    |2024-05-31|523.59| 527.5|518.36|527.37|90785755|   SPY|
    |2024-05-30|524.52| 525.2|521.33|522.61|46468510|   SPY|
    |2024-05-29|525.68|527.31|525.37| 526.1|45190323|   SPY|
    +----------+------+------+------+------+--------+------+
    """
    @classmethod
    def name(self) -> str:
        return "stock"

    def schema(self) -> str:
        return (
            "date string, open double, high double, "
            "low double, close double, volume long, symbol string"
        )

    def reader(self, schema):
        return StockDataReader(schema, self.options)


@dataclass
class Symbol(InputPartition):
    name: str


class StockDataReader(DataSourceReader):
    def __init__(self, schema: StructType, options: Dict):
        self.schema = schema
        self.options = options
        self.api_key = options.get("api_key")
        if not self.api_key:
            raise Exception(f"API Key is required to load the data.")
        # The name of the time-series. See https://www.alphavantage.co/documentation/ for more info.
        self.function = options.get("function", "TIME_SERIES_DAILY")
        if self.function not in ("TIME_SERIES_DAILY", "TIME_SERIES_WEEKLY", "TIME_SERIES_MONTHLY"):
            raise Exception(f"Function `{self.function}` is not supported.")

    def partitions(self):
        names = self.options["path"]
        # Split the names by comma and create a partition for each symbol.
        return [Symbol(name.strip()) for name in names.split(",")]

    def read(self, partition: Symbol):
        import requests
        symbol = partition.name
        resp = requests.get(
            f"https://www.alphavantage.co/query?"
            f"function={self.function}&symbol={symbol}&apikey={self.api_key}"
        )
        resp.raise_for_status()
        data = resp.json()
        key_name = next(key for key in data.keys() if key != "Meta Data")
        for date, info in data[key_name].items():
            yield (
                date,
                float(info["1. open"]),
                float(info["2. high"]),
                float(info["3. low"]),
                float(info["4. close"]),
                float(info["5. volume"]),
                symbol
            )

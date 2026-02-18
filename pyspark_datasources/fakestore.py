"""Fake Store API data source - e-commerce test data."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class FakeStoreDataSource(DataSource):
    """
    A DataSource for reading products from fakestoreapi.com.
    No API key. Name: `fakestore`
    Schema: `id int, title string, price double, category string, rating double`
    """

    @classmethod
    def name(cls):
        return "fakestore"

    def schema(self):
        return "id int, title string, price double, category string, rating double"

    def reader(self, schema):
        return FakeStoreReader(self.options)


class FakeStoreReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.endpoint = options.get("endpoint", "products")

    def read(self, partition):
        try:
            url = f"https://fakestoreapi.com/{self.endpoint}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            items = data if isinstance(data, list) else [data]
            for p in items:
                rating = p.get("rating") or {}
                rating_val = rating.get("rate") if isinstance(rating, dict) else 0
                yield Row(
                    id=p.get("id", 0),
                    title=(p.get("title", ""))[:200],
                    price=float(p.get("price", 0)),
                    category=p.get("category", ""),
                    rating=float(rating_val) if rating_val else 0.0,
                )
        except requests.RequestException:
            return iter([])

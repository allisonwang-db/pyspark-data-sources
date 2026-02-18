"""Cocktail DB API data source - reads cocktail recipes."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class CocktailDataSource(DataSource):
    """
    A DataSource for reading cocktail data from TheCocktailDB.
    No API key required for basic usage. Name: `cocktail`
    Schema: `id string, name string, category string, alcoholic string, glass string`
    """

    @classmethod
    def name(cls):
        return "cocktail"

    def schema(self):
        return "id string, name string, category string, alcoholic string, glass string"

    def reader(self, schema):
        return CocktailReader(self.options)


class CocktailReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.search = options.get("path") or options.get("search", "a")

    def read(self, partition):
        try:
            response = requests.get(
                "https://www.thecocktaildb.com/api/json/v1/1/search.php",
                params={"s": self.search},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            drinks = data.get("drinks") or []
            for d in drinks[:100]:
                yield Row(
                    id=d.get("idDrink", ""),
                    name=d.get("strDrink", ""),
                    category=d.get("strCategory", ""),
                    alcoholic=d.get("strAlcoholic", ""),
                    glass=d.get("strGlass", ""),
                )
        except requests.RequestException:
            return iter([])

"""Pokemon API data source - reads Pokemon from PokeAPI."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class PokemonDataSource(DataSource):
    """
    A DataSource for reading Pokemon from pokeapi.co.
    No API key required. Name: `pokemon`
    Schema: `name string, url string, id int`
    """

    @classmethod
    def name(cls):
        return "pokemon"

    def schema(self):
        return "name string, url string, id int"

    def reader(self, schema):
        return PokemonReader(self.options)


class PokemonReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.limit = min(int(options.get("limit", "50")), 200)

    def read(self, partition):
        try:
            response = requests.get(
                "https://pokeapi.co/api/v2/pokemon",
                params={"limit": self.limit},
                timeout=30,
            )
            response.raise_for_status()
            for i, p in enumerate(response.json().get("results", []), 1):
                yield Row(
                    name=p.get("name", ""),
                    url=p.get("url", ""),
                    id=i,
                )
        except requests.RequestException:
            return iter([])

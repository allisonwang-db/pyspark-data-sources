"""Dog CEO API data source - reads dog breed data."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class DogDataSource(DataSource):
    """
    A DataSource for reading dog breed data from the Dog CEO API.

    No API key required. Lists all breeds or fetches random images.

    Name: `dog`

    Schema: `breed string, sub_breed string`

    Examples
    --------
    >>> spark.read.format("dog").load().show()
    """

    @classmethod
    def name(cls):
        return "dog"

    def schema(self):
        return "breed string, sub_breed string"

    def reader(self, schema):
        return DogReader(self.options)


class DogReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.endpoint = options.get("endpoint", "breeds")

    def read(self, partition):
        try:
            if self.endpoint == "breeds":
                return self._read_breeds()
            return iter([])
        except requests.RequestException:
            return iter([])

    def _read_breeds(self):
        response = requests.get("https://dog.ceo/api/breeds/list/all", timeout=30)
        response.raise_for_status()
        data = response.json()
        breeds = data.get("message", {})
        for breed, sub_list in breeds.items():
            if sub_list:
                for sub in sub_list:
                    yield Row(breed=breed, sub_breed=sub)
            else:
                yield Row(breed=breed, sub_breed="")

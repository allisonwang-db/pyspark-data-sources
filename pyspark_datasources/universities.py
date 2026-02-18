"""Universities API data source - reads university data from HipoLabs."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class UniversitiesDataSource(DataSource):
    """
    A DataSource for reading university data from universities.hipolabs.com.
    No API key required. Name: `universities`
    Schema: `name string, country string, alpha_two_code string, domains string`
    """

    @classmethod
    def name(cls):
        return "universities"

    def schema(self):
        return "name string, country string, alpha_two_code string, domains string"

    def reader(self, schema):
        return UniversitiesReader(self.options)


class UniversitiesReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.country = options.get("path") or options.get("country", "")

    def read(self, partition):
        try:
            params = {}
            if self.country:
                params["country"] = self.country
            response = requests.get(
                "http://universities.hipolabs.com/search",
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            for u in response.json():
                domains = ",".join(u.get("domains", []))
                yield Row(
                    name=u.get("name", ""),
                    country=u.get("country", ""),
                    alpha_two_code=u.get("alpha_two_code", ""),
                    domains=domains,
                )
        except requests.RequestException:
            return iter([])

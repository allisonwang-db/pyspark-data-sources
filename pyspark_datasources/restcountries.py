"""REST Countries data source - reads country data from restcountries.com API."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader

FIELDS = "name,capital,region,subregion,population,area,languages,currencies"


class RestCountriesDataSource(DataSource):
    """
    A DataSource for reading country data from the REST Countries API.

    No API key required. Supports reading all countries or filtering by name/code.

    Name: `restcountries`

    Schema: `name_common string, name_official string, capital string, region string,
    subregion string, population long, area double, languages string, currencies string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import RestCountriesDataSource
    >>> spark.dataSource.register(RestCountriesDataSource)

    Load all countries.

    >>> spark.read.format("restcountries").load().show()

    Load specific country by name.

    >>> spark.read.format("restcountries").load("usa").show()

    Load by country code.

    >>> spark.read.format("restcountries").load("de").show()
    """

    @classmethod
    def name(cls):
        return "restcountries"

    def schema(self):
        return (
            "name_common string, name_official string, capital string, region string, "
            "subregion string, population long, area double, languages string, currencies string"
        )

    def reader(self, schema):
        return RestCountriesReader(self.options)


class RestCountriesReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.path = options.get("path")
        self.base_url = "https://restcountries.com/v3.1"

    def read(self, partition):
        if self.path:
            url = f"{self.base_url}/name/{self.path}"
        else:
            url = f"{self.base_url}/all"

        try:
            response = requests.get(url, params={"fields": FIELDS}, timeout=30)
            response.raise_for_status()
            countries = response.json()

            if isinstance(countries, dict) and countries.get("status") == 404:
                return iter([])

            if isinstance(countries, dict):
                countries = [countries]

            for c in countries:
                yield self._to_row(c)
        except requests.RequestException:
            return iter([])

    def _to_row(self, c):
        name = c.get("name", {})
        capital = c.get("capital") or []
        capital_str = capital[0] if capital else ""
        languages = c.get("languages") or {}
        currencies = c.get("currencies") or {}
        return Row(
            name_common=name.get("common", ""),
            name_official=name.get("official", ""),
            capital=capital_str,
            region=c.get("region", ""),
            subregion=c.get("subregion", ""),
            population=c.get("population") or 0,
            area=float(c.get("area") or 0),
            languages=",".join(f"{k}:{v}" for k, v in languages.items()),
            currencies=",".join(f"{k}:{v.get('name','')}" for k, v in currencies.items()),
        )

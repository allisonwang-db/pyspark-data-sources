"""OpenFDA data source - reads drug, food, and device data from the FDA API."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class OpenFdaDataSource(DataSource):
    """
    A DataSource for reading data from the OpenFDA API.

    No API key required for reasonable usage. Supports drug labels, food recalls,
    device events, and more.

    Name: `openfda`

    Schema: `id string, brand_name string, generic_name string, manufacturer_name string,
    product_type string, purpose string, active_ingredient string, effective_time string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import OpenFdaDataSource
    >>> spark.dataSource.register(OpenFdaDataSource)

    Load drug labels (default, limit 100).

    >>> spark.read.format("openfda").load().show()

    Load food recalls.

    >>> spark.read.format("openfda").option("endpoint", "food/event").load().show()

    Limit results.

    >>> spark.read.format("openfda").option("limit", "50").load().show()
    """

    @classmethod
    def name(cls):
        return "openfda"

    def schema(self):
        return (
            "id string, brand_name string, generic_name string, manufacturer_name string, "
            "product_type string, purpose string, active_ingredient string, effective_time string"
        )

    def reader(self, schema):
        return OpenFdaReader(self.options)


class OpenFdaReader(DataSourceReader):
    ENDPOINTS = ("drug/label", "food/event", "device/event")

    def __init__(self, options):
        self.options = options
        self.endpoint = self.options.get("endpoint", "drug/label")
        self.limit = int(self.options.get("limit", "100"))
        self.limit = min(max(self.limit, 1), 1000)

    def read(self, partition):
        url = f"https://api.fda.gov/{self.endpoint}.json"
        try:
            response = requests.get(
                url, params={"limit": self.limit}, timeout=30
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])

            for r in results:
                yield self._to_row(r)
        except requests.RequestException:
            return iter([])

    def _to_row(self, r):
        openfda = r.get("openfda") or {}
        purpose = r.get("purpose") or []
        active = r.get("active_ingredient") or []

        def first(lst):
            return lst[0] if lst else ""

        return Row(
            id=r.get("id", ""),
            brand_name=first(openfda.get("brand_name")),
            generic_name=first(openfda.get("generic_name")),
            manufacturer_name=first(openfda.get("manufacturer_name")),
            product_type=first(openfda.get("product_type")),
            purpose=first(purpose),
            active_ingredient=first(active),
            effective_time=r.get("effective_time", ""),
        )

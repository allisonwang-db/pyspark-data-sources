"""Met Museum API data source - artwork from Metropolitan Museum of Art."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class MetMuseumDataSource(DataSource):
    """
    A DataSource for reading artwork from metmuseum.org API.
    No API key. Name: `metmuseum`
    Schema: `objectID int, title string, artistDisplayName string,
    objectDate string, department string`
    """

    @classmethod
    def name(cls):
        return "metmuseum"

    def schema(self):
        return (
            "objectID int, title string, artistDisplayName string, "
            "objectDate string, department string"
        )

    def reader(self, schema):
        return MetMuseumReader(self.options)


class MetMuseumReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.limit = min(int(options.get("limit", "50")), 100)

    def read(self, partition):
        try:
            response = requests.get(
                "https://collectionapi.metmuseum.org/public/collection/v1/objects",
                params={"departmentIds": 1},
                timeout=30,
            )
            response.raise_for_status()
            ids = response.json().get("objectIDs", [])[: self.limit]
            for oid in ids:
                r = requests.get(
                    f"https://collectionapi.metmuseum.org/public/collection/v1/objects/{oid}",
                    timeout=10,
                )
                if r.status_code != 200:
                    continue
                o = r.json()
                yield Row(
                    objectID=o.get("objectID", 0),
                    title=(o.get("title", ""))[:200],
                    artistDisplayName=(o.get("artistDisplayName", ""))[:100],
                    objectDate=o.get("objectDate", ""),
                    department=o.get("department", ""),
                )
        except requests.RequestException:
            return iter([])

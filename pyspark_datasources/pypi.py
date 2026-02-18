"""PyPI data source - reads Python package metadata from PyPI."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class PyPiDataSource(DataSource):
    """
    A DataSource for reading Python package metadata from PyPI.

    No API key required. Path specifies package name (e.g. requests).

    Name: `pypi`

    Schema: `package_name string, version string, summary string, author string,
    license string, requires_python string, upload_time string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import PyPiDataSource
    >>> spark.dataSource.register(PyPiDataSource)

    Load package info for requests.

    >>> spark.read.format("pypi").load("requests").show()
    """

    @classmethod
    def name(cls):
        return "pypi"

    def schema(self):
        return (
            "package_name string, version string, summary string, author string, "
            "license string, requires_python string, upload_time string"
        )

    def reader(self, schema):
        return PyPiReader(self.options)


class PyPiReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.package = self.options.get("path") or "requests"

    def read(self, partition):
        url = f"https://pypi.org/pypi/{self.package}/json"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "message" in data and "404" in str(data.get("message", "")):
                return iter([])

            info = data.get("info", {})
            # Get latest version info from releases
            releases = data.get("releases", {})
            version = info.get("version", "")
            upload_time = ""

            if version and version in releases:
                files = releases[version]
                if files:
                    upload_time = files[0].get("upload_time", "")

            yield Row(
                package_name=info.get("name", ""),
                version=version,
                summary=(info.get("summary") or "")[:500],
                author=info.get("author", ""),
                license=info.get("license", ""),
                requires_python=info.get("requires_python", ""),
                upload_time=upload_time,
            )
        except requests.RequestException:
            return iter([])

"""Docker Hub data source - reads image tags from Docker Hub API."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class DockerHubDataSource(DataSource):
    """
    A DataSource for reading Docker image tags from Docker Hub.

    No API key required for public images. Path = repository (e.g. library/redis).

    Name: `dockerhub`

    Schema: `name string, digest string, full_size long, last_updated string,
    last_updater_username string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import DockerHubDataSource
    >>> spark.dataSource.register(DockerHubDataSource)

    Load tags for redis image.

    >>> spark.read.format("dockerhub").load("library/redis").show()

    Limit page size.

    >>> spark.read.format("dockerhub").option("page_size", "50").load("nginx").show()
    """

    @classmethod
    def name(cls):
        return "dockerhub"

    def schema(self):
        return (
            "name string, digest string, full_size long, last_updated string, "
            "last_updater_username string"
        )

    def reader(self, schema):
        return DockerHubReader(self.options)


class DockerHubReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        path = self.options.get("path") or "library/nginx"
        if "/" not in path:
            path = f"library/{path}"
        self.repository = path
        self.page_size = min(int(self.options.get("page_size", "100")), 100)

    def read(self, partition):
        url = f"https://hub.docker.com/v2/repositories/{self.repository}/tags"
        try:
            response = requests.get(
                url, params={"page_size": self.page_size}, timeout=30
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])

            for r in results:
                yield Row(
                    name=r.get("name", ""),
                    digest=r.get("digest", ""),
                    full_size=r.get("full_size", 0),
                    last_updated=r.get("last_updated", ""),
                    last_updater_username=r.get("last_updater_username", ""),
                )
        except requests.RequestException:
            return iter([])

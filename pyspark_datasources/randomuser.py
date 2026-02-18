"""Random User API data source - generates fake user profiles for testing."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class RandomUserDataSource(DataSource):
    """
    A DataSource for reading fake user profiles from the Random User API.

    No API key required. Useful for testing and generating sample data.

    Name: `randomuser`

    Schema: `gender string, first_name string, last_name string, email string,
    city string, country string, phone string, dob string, nat string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import RandomUserDataSource
    >>> spark.dataSource.register(RandomUserDataSource)

    Load 10 random users (default).

    >>> spark.read.format("randomuser").load().show()

    Load 100 users.

    >>> spark.read.format("randomuser").option("results", "100").load().show()
    """

    @classmethod
    def name(cls):
        return "randomuser"

    def schema(self):
        return (
            "gender string, first_name string, last_name string, email string, "
            "city string, country string, phone string, dob string, nat string"
        )

    def reader(self, schema):
        return RandomUserReader(self.options)


class RandomUserReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.results = int(self.options.get("results", "10"))
        self.results = min(max(self.results, 1), 5000)  # API limit 1-5000

    def read(self, partition):
        try:
            response = requests.get(
                "https://randomuser.me/api/",
                params={"results": self.results},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])

            for u in results:
                name = u.get("name", {})
                loc = u.get("location", {})
                dob = u.get("dob", {})
                yield Row(
                    gender=u.get("gender", ""),
                    first_name=name.get("first", ""),
                    last_name=name.get("last", ""),
                    email=u.get("email", ""),
                    city=loc.get("city", ""),
                    country=loc.get("country", ""),
                    phone=u.get("phone", ""),
                    dob=dob.get("date", ""),
                    nat=u.get("nat", ""),
                )
        except requests.RequestException:
            return iter([])

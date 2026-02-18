"""ReqRes API data source - test user data from reqres.in."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class ReqresDataSource(DataSource):
    """
    A DataSource for reading users from reqres.in.
    No API key. Name: `reqres`
    Schema: `id int, email string, first_name string, last_name string, avatar string`
    """

    @classmethod
    def name(cls):
        return "reqres"

    def schema(self):
        return "id int, email string, first_name string, last_name string, avatar string"

    def reader(self, schema):
        return ReqresReader(self.options)


class ReqresReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.page = int(options.get("page", "1"))

    def read(self, partition):
        try:
            response = requests.get(
                f"https://reqres.in/api/users?page={self.page}",
                timeout=30,
            )
            response.raise_for_status()
            for u in response.json().get("data", []):
                yield Row(
                    id=u.get("id", 0),
                    email=u.get("email", ""),
                    first_name=u.get("first_name", ""),
                    last_name=u.get("last_name", ""),
                    avatar=u.get("avatar", ""),
                )
        except requests.RequestException:
            return iter([])

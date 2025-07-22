# pyspark_datasources/jsonplaceholder.py

from typing import Dict, Any, List, Iterator
import requests
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType
from pyspark.sql import Row


class JSONPlaceholderDataSource(DataSource):
    """
    A PySpark data source for JSONPlaceholder API.

    JSONPlaceholder is a free fake REST API for testing and prototyping.
    This data source provides access to posts, users, todos, comments, albums, and photos.

    Supported endpoints:
    - posts: Blog posts with userId, id, title, body
    - users: User profiles with complete information
    - todos: Todo items with userId, id, title, completed
    - comments: Comments with postId, id, name, email, body
    - albums: Albums with userId, id, title
    - photos: Photos with albumId, id, title, url, thumbnailUrl

    Name: `jsonplaceholder`

    Examples
    --------
    Register the data source:

    >>> spark.dataSource.register(JSONPlaceholderDataSource)

    Read posts (default):

    >>> spark.read.format("jsonplaceholder").load().show()

    Read users:

    >>> spark.read.format("jsonplaceholder").option("endpoint", "users").load().show()

    Read with limit:

    >>> spark.read.format("jsonplaceholder").option("endpoint", "todos").option("limit", "5").load().show()

    Read specific item:

    >>> spark.read.format("jsonplaceholder").option("endpoint", "posts").option("id", "1").load().show()
    """

    @classmethod
    def name(cls) -> str:
        return "jsonplaceholder"

    def __init__(self, options=None):
        self.options = options or {}

    def schema(self) -> str:
        endpoint = self.options.get("endpoint", "posts")

        if endpoint == "posts":
            return "userId INT, id INT, title STRING, body STRING"
        elif endpoint == "users":
            return ("id INT, name STRING, username STRING, email STRING, phone STRING, "
                    "website STRING, address_street STRING, address_suite STRING, "
                    "address_city STRING, address_zipcode STRING, address_geo_lat STRING, "
                    "address_geo_lng STRING, company_name STRING, company_catchPhrase STRING, "
                    "company_bs STRING")
        elif endpoint == "todos":
            return "userId INT, id INT, title STRING, completed BOOLEAN"
        elif endpoint == "comments":
            return "postId INT, id INT, name STRING, email STRING, body STRING"
        elif endpoint == "albums":
            return "userId INT, id INT, title STRING"
        elif endpoint == "photos":
            return "albumId INT, id INT, title STRING, url STRING, thumbnailUrl STRING"
        else:
            return "userId INT, id INT, title STRING, body STRING"

    def reader(self, schema: StructType) -> DataSourceReader:
        return JSONPlaceholderReader(self.options)


class JSONPlaceholderReader(DataSourceReader):
    """Reader implementation for JSONPlaceholder API"""

    def __init__(self, options: Dict[str, str]):
        self.options = options
        self.base_url = "https://jsonplaceholder.typicode.com"

        self.endpoint = self.options.get("endpoint", "posts")
        self.limit = self.options.get("limit")
        self.id = self.options.get("id")

    def partitions(self) -> List[InputPartition]:
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[Row]:
        url = f"{self.base_url}/{self.endpoint}"

        if self.id:
            url += f"/{self.id}"

        params = {}
        if self.limit and not self.id:
            params["_limit"] = self.limit

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                data = []

            processed_data = []
            for item in data:
                processed_item = self._process_item(item)
                processed_data.append(processed_item)

            return iter(processed_data)

        except Exception:
            return iter([])

    def _process_item(self, item: Dict[str, Any]) -> Row:
        """Process individual items based on endpoint type"""

        if self.endpoint == "posts":
            return Row(
                userId=item.get("userId"),
                id=item.get("id"),
                title=item.get("title", ""),
                body=item.get("body", "")
            )

        elif self.endpoint == "users":
            address = item.get("address", {})
            geo = address.get("geo", {})
            company = item.get("company", {})

            return Row(
                id=item.get("id"),
                name=item.get("name", ""),
                username=item.get("username", ""),
                email=item.get("email", ""),
                phone=item.get("phone", ""),
                website=item.get("website", ""),
                address_street=address.get("street", ""),
                address_suite=address.get("suite", ""),
                address_city=address.get("city", ""),
                address_zipcode=address.get("zipcode", ""),
                address_geo_lat=geo.get("lat", ""),
                address_geo_lng=geo.get("lng", ""),
                company_name=company.get("name", ""),
                company_catchPhrase=company.get("catchPhrase", ""),
                company_bs=company.get("bs", "")
            )

        elif self.endpoint == "todos":
            return Row(
                userId=item.get("userId"),
                id=item.get("id"),
                title=item.get("title", ""),
                completed=item.get("completed", False)
            )

        elif self.endpoint == "comments":
            return Row(
                postId=item.get("postId"),
                id=item.get("id"),
                name=item.get("name", ""),
                email=item.get("email", ""),
                body=item.get("body", "")
            )

        elif self.endpoint == "albums":
            return Row(
                userId=item.get("userId"),
                id=item.get("id"),
                title=item.get("title", "")
            )

        elif self.endpoint == "photos":
            return Row(
                albumId=item.get("albumId"),
                id=item.get("id"),
                title=item.get("title", ""),
                url=item.get("url", ""),
                thumbnailUrl=item.get("thumbnailUrl", "")
            )

        else:
            return Row(
                userId=item.get("userId"),
                id=item.get("id"),
                title=item.get("title", ""),
                body=item.get("body", "")
            )
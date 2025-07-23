from typing import Dict, Any, List, Iterator
import requests
import logging
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

    Referential Integrity
    -------------------
    The data source supports joining related datasets:

    1. Posts and Users relationship:
        posts.userId = users.id
        >>> posts_df = spark.read.format("jsonplaceholder").option("endpoint", "posts").load()
        >>> users_df = spark.read.format("jsonplaceholder").option("endpoint", "users").load()
        >>> posts_with_authors = posts_df.join(users_df, posts_df.userId == users_df.id)

    2. Posts and Comments relationship:
        comments.postId = posts.id
        >>> comments_df = spark.read.format("jsonplaceholder").option("endpoint", "comments").load()
        >>> posts_with_comments = posts_df.join(comments_df, posts_df.id == comments_df.postId)

    3. Users, Albums and Photos relationship:
        albums.userId = users.id
        photos.albumId = albums.id
        >>> albums_df = spark.read.format("jsonplaceholder").option("endpoint", "albums").load()
        >>> photos_df = spark.read.format("jsonplaceholder").option("endpoint", "photos").load()
        >>> user_albums = users_df.join(albums_df, users_df.id == albums_df.userId)
        >>> user_photos = user_albums.join(photos_df, albums_df.id == photos_df.albumId)
    """

    @classmethod
    def name(cls) -> str:
        return "jsonplaceholder"

    def __init__(self, options=None):
        self.options = options or {}

    def schema(self) -> str:
        """ Returns the schema for the selected endpoint."""
        schemas = {
            "posts": "userId INT, id INT, title STRING, body STRING",
            "users": ("id INT, name STRING, username STRING, email STRING, phone STRING, "
                      "website STRING, address_street STRING, address_suite STRING, "
                      "address_city STRING, address_zipcode STRING, address_geo_lat STRING, "
                      "address_geo_lng STRING, company_name STRING, company_catchPhrase STRING, "
                      "company_bs STRING"),
            "todos": "userId INT, id INT, title STRING, completed BOOLEAN",
            "comments": "postId INT, id INT, name STRING, email STRING, body STRING",
            "albums": "userId INT, id INT, title STRING",
            "photos": "albumId INT, id INT, title STRING, url STRING, thumbnailUrl STRING"
        }

        endpoint = self.options.get("endpoint", "posts")
        return schemas.get(endpoint, schemas["posts"])

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

            return iter([self._process_item(item) for item in data])

        except requests.RequestException as e:
            logging.warning(f"Failed to fetch data from {url}: {e}")
            return iter([])
        except ValueError as e:
            logging.warning(f"Failed to parse JSON from {url}: {e}")
            return iter([])
        except Exception as e:
            logging.error(f"Unexpected error while reading data: {e}")
            return iter([])

    def _process_item(self, item: Dict[str, Any]) -> Row:
        """Process individual items based on endpoint type"""

        def _process_posts(item):
            return Row(
                userId=item.get("userId"),
                id=item.get("id"),
                title=item.get("title", ""),
                body=item.get("body", "")
            )

        def _process_users(item):
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

        def _process_todos(item):
            return Row(
                userId=item.get("userId"),
                id=item.get("id"),
                title=item.get("title", ""),
                completed=item.get("completed", False)
            )

        def _process_comments(item):
            return Row(
                postId=item.get("postId"),
                id=item.get("id"),
                name=item.get("name", ""),
                email=item.get("email", ""),
                body=item.get("body", "")
            )

        def _process_albums(item):
            return Row(
                userId=item.get("userId"),
                id=item.get("id"),
                title=item.get("title", "")
            )

        def _process_photos(item):
            return Row(
                albumId=item.get("albumId"),
                id=item.get("id"),
                title=item.get("title", ""),
                url=item.get("url", ""),
                thumbnailUrl=item.get("thumbnailUrl", "")
            )

        processors = {
            "posts": _process_posts,
            "users": _process_users,
            "todos": _process_todos,
            "comments": _process_comments,
            "albums": _process_albums,
            "photos": _process_photos
        }

        processor = processors.get(self.endpoint, _process_posts)
        return processor(item)
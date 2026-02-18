"""Reddit data source - reads posts from subreddits."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader

USER_AGENT = "pyspark-datasources/1.0"


class RedditDataSource(DataSource):
    """
    A DataSource for reading posts from Reddit subreddits.

    No API key required for public subreddits. Path specifies subreddit (e.g. python).

    Name: `reddit`

    Schema: `id string, title string, author string, score int, num_comments int,
    created_utc long, url string, selftext string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import RedditDataSource
    >>> spark.dataSource.register(RedditDataSource)

    Load posts from r/python.

    >>> spark.read.format("reddit").load("python").show()

    Limit results.

    >>> spark.read.format("reddit").option("limit", "25").load("programming").show()
    """

    @classmethod
    def name(cls):
        return "reddit"

    def schema(self):
        return (
            "id string, title string, author string, score int, num_comments int, "
            "created_utc long, url string, selftext string"
        )

    def reader(self, schema):
        return RedditReader(self.options)


class RedditReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.subreddit = self.options.get("path") or "python"
        self.limit = min(int(self.options.get("limit", "25")), 100)

    def read(self, partition):
        url = f"https://www.reddit.com/r/{self.subreddit}/.json"
        headers = {"User-Agent": USER_AGENT}
        try:
            response = requests.get(
                url, headers=headers, params={"limit": self.limit}, timeout=30
            )
            response.raise_for_status()
            data = response.json()
            children = data.get("data", {}).get("children", [])

            for child in children:
                d = child.get("data", {})
                yield Row(
                    id=d.get("id", ""),
                    title=d.get("title", ""),
                    author=d.get("author", ""),
                    score=d.get("score", 0),
                    num_comments=d.get("num_comments", 0),
                    created_utc=d.get("created_utc", 0),
                    url=d.get("url", ""),
                    selftext=(d.get("selftext") or "")[:1000],
                )
        except requests.RequestException:
            return iter([])

import requests

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class GithubDataSource(DataSource):

    @classmethod
    def name(self):
        return "github"

    def schema(self):
        return "id int, title string, author string, created_at string, updated_at string"

    def reader(self, schema):
        return GithubPullRequestReader(self.options)


class GithubPullRequestReader(DataSourceReader):
    def __init__(self, options):
        self.token = options.get("token")
        self.repo = options.get("path")
        if self.repo is None:
            raise Exception(f"Must specify a repo in `.load()` method.")

    def read(self, partition):
        header = {
            "Accept": "application/vnd.github+json",
        }
        if self.token is not None:
            header["Authorization"] = f"Bearer {self.token}"
        url = f"https://api.github.com/repos/{self.repo}/pulls"
        response = requests.get(url)
        response.raise_for_status()
        prs = response.json()
        for pr in prs:
            yield Row(
                id = pr.get("number"),
                title = pr.get("title"),
                author = pr.get("user", {}).get("login"),
                created_at = pr.get("created_at"),
                updated_at = pr.get("updated_at")
            )


"""Open Trivia Database data source."""

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class TriviaDataSource(DataSource):
    """
    A DataSource for reading trivia questions from opentdb.com.
    No API key. Name: `trivia`
    Schema: `category string, type string, difficulty string, question string,
    correct_answer string`
    """

    @classmethod
    def name(cls):
        return "trivia"

    def schema(self):
        return (
            "category string, type string, difficulty string, question string, "
            "correct_answer string"
        )

    def reader(self, schema):
        return TriviaReader(self.options)


class TriviaReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.amount = min(int(options.get("amount", "10")), 50)

    def read(self, partition):
        try:
            import html

            response = requests.get(
                "https://opentdb.com/api.php",
                params={"amount": self.amount, "type": "multiple"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            if data.get("response_code", 1) != 0:
                return iter([])
            for q in data.get("results", []):
                yield Row(
                    category=q.get("category", ""),
                    type=q.get("type", ""),
                    difficulty=q.get("difficulty", ""),
                    question=html.unescape(q.get("question", "")[:500]),
                    correct_answer=html.unescape(q.get("correct_answer", "")),
                )
        except requests.RequestException:
            return iter([])

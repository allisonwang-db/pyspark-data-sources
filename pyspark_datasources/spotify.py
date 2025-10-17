
import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    ArrayType,
)


class SpotifyDataSource(DataSource):
    """
    A DataSource for reading data from Spotify.

    Name: `spotify`

    The `type` option is used to specify what data to load. Currently, only
    the `tracks` type is supported, which loads the user's saved songs.

    Schema for `tracks` type:
    - `id`: string
    - `name`: string
    - `artists`: array<string>
    - `album`: string
    - `duration_ms`: long
    - `popularity`: integer
    - `added_at`: string

    Examples:
    ---------
    Register the data source.

    >>> from pyspark_datasources import SpotifyDataSource
    >>> spark.dataSource.register(SpotifyDataSource)

    Load your saved tracks from Spotify.

    >>> df = (
    ...     spark.read.format("spotify")
    ...     .option("spotify.client.id", "YOUR_CLIENT_ID")
    ...     .option("spotify.client.secret", "YOUR_CLIENT_SECRET")
    ...     .option("spotify.refresh.token", "YOUR_REFRESH_TOKEN")
    ...     .option("type", "tracks")
    ...     .load()
    ... )
    >>> df.show()
    +----------------------+--------------------+--------------------+--------------------+-----------+----------+--------------------+
    |                    id|                name|             artists|               album|duration_ms|popularity|            added_at|
    +----------------------+--------------------+--------------------+--------------------+-----------+----------+--------------------+
    |1BxfuPKGuaTgP7aM0B...|           All Too Well|      [Taylor Swift]|                 Red|     329466|        82|2025-10-16T12:00:00Z|
    |                  ...|                 ...|                 ...|                 ...|        ...|       ...|                 ...|
    +----------------------+--------------------+--------------------+--------------------+-----------+----------+--------------------+
    """

    @classmethod
    def name(cls):
        return "spotify"

    def schema(self):
        # Simplified schema for tracks
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField(
                    "artists", ArrayType(StringType(), True), True
                ),
                StructField("album", StringType(), True),
                StructField("duration_ms", LongType(), True),
                StructField("popularity", IntegerType(), True),
                StructField("added_at", StringType(), True),
            ]
        )

    def reader(self, schema):
        return SpotifyReader(self.options)


class SpotifyReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.client_id = self.options.get("spotify.client.id")
        self.client_secret = self.options.get("spotify.client.secret")
        self.refresh_token = self.options.get("spotify.refresh.token")
        self.type = self.options.get("type", "tracks")

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError(
                "spotify.client.id, spotify.client.secret, and spotify.refresh.token must be specified in options"
            )

    def read(self, partition):
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        if self.type == "tracks":
            url = "https://api.spotify.com/v1/me/tracks"
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                for item in data["items"]:
                    track = item["track"]
                    yield Row(
                        id=track["id"],
                        name=track["name"],
                        artists=[artist["name"] for artist in track["artists"]],
                        album=track["album"]["name"],
                        duration_ms=track["duration_ms"],
                        popularity=track["popularity"],
                        added_at=item["added_at"],
                    )
                url = data.get("next")
        else:
            raise ValueError(f"Unsupported type: {self.type}")

    def _get_access_token(self):
        response = requests.post(
            "https://accounts.spotify.com/api/token",
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token},
            auth=(self.client_id, self.client_secret),
        )
        response.raise_for_status()
        return response.json()["access_token"]


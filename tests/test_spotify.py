"""
Unit tests for the Spotify data source.

These tests focus on the SpotifyReader class directly to avoid issues with
SparkSession creation in certain test environments. This approach allows for
testing the core logic of the data source, including authentication and data
parsing, without a full SparkSession.
"""
import pytest
from unittest.mock import patch, MagicMock
import requests
from pyspark.sql import Row
from pyspark_datasources.spotify import SpotifyReader

@patch("pyspark_datasources.spotify.requests.post")
@patch("pyspark_datasources.spotify.requests.get")
def test_spotify_reader_success(mock_get, mock_post):
    # Mock the response from the token endpoint
    mock_post.return_value.json.return_value = {"access_token": "test_token"}
    mock_post.return_value.raise_for_status = MagicMock()

    # Mock the response from the tracks endpoint (with pagination)
    mock_get.side_effect = [
        MagicMock(
            json=lambda: {
                "items": [
                    {
                        "added_at": "2025-10-16T12:00:00Z",
                        "track": {
                            "id": "track1",
                            "name": "Test Track 1",
                            "artists": [{"name": "Artist 1"}],
                            "album": {"name": "Album 1"},
                            "duration_ms": 200000,
                            "popularity": 50,
                        },
                    }
                ],
                "next": "https://api.spotify.com/v1/me/tracks?offset=1&limit=1",
            },
            raise_for_status=MagicMock(),
        ),
        MagicMock(
            json=lambda: {
                "items": [
                    {
                        "added_at": "2025-10-16T12:05:00Z",
                        "track": {
                            "id": "track2",
                            "name": "Test Track 2",
                            "artists": [{"name": "Artist 2"}],
                            "album": {"name": "Album 2"},
                            "duration_ms": 220000,
                            "popularity": 60,
                        },
                    }
                ],
                "next": None,
            },
            raise_for_status=MagicMock(),
        ),
    ]

    options = {
        "spotify.client.id": "test_id",
        "spotify.client.secret": "test_secret",
        "spotify.refresh.token": "test_refresh",
    }
    reader = SpotifyReader(options)
    rows = list(reader.read(None))

    assert len(rows) == 2
    assert rows[0]["name"] == "Test Track 1"
    assert rows[1]["name"] == "Test Track 2"

def test_spotify_reader_missing_credentials():
    with pytest.raises(ValueError, match="must be specified in options"):
        SpotifyReader({})

@patch("pyspark_datasources.spotify.requests.post")
@patch("pyspark_datasources.spotify.requests.get")
def test_spotify_reader_api_error(mock_get, mock_post):
    mock_post.return_value.json.return_value = {"access_token": "test_token"}
    mock_post.return_value.raise_for_status = MagicMock()

    mock_get.return_value.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "401 Client Error: Unauthorized for url"
    )

    options = {
        "spotify.client.id": "test_id",
        "spotify.client.secret": "test_secret",
        "spotify.refresh.token": "test_refresh",
    }
    reader = SpotifyReader(options)
    with pytest.raises(requests.exceptions.HTTPError):
        list(reader.read(None))
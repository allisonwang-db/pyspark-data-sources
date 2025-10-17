
# Spotify

The Spotify data source allows you to read data from the Spotify API as a Spark DataFrame. Currently, it supports reading a user's saved tracks.

## Authentication

To use the Spotify data source, you need to authenticate with the Spotify API. This is done using OAuth 2.0. You will need to provide your Client ID, Client Secret, and a Refresh Token.

### 1. Create a Spotify Developer App

1.  Go to the [Spotify Developer Dashboard](https://developer.spotify.com/dashboard) and log in.
2.  Click on "Create an App".
3.  Give your app a name and description, and agree to the terms.
4.  Once the app is created, you will see your **Client ID** and **Client Secret**. Copy these values.
5.  Click on "Edit Settings" and add a **Redirect URI**. For the purpose of generating a refresh token, you can use `https://example.com/callback`. Click "Save".

### 2. Generate a Refresh Token

A refresh token is a long-lived credential that can be used to obtain new access tokens. You only need to generate this once.

To generate a refresh token, you can use the following Python script. You will need to install the `spotipy` library (`pip install spotipy`).

```python
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Replace with your Client ID, Client Secret, and Redirect URI
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"
REDIRECT_URI = "https://example.com/callback"

# The scope determines what permissions your app is requesting.
# For reading saved tracks, you need the 'user-library-read' scope.
SCOPE = "user-library-read"

auth_manager = SpotifyOAuth(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    scope=SCOPE,
    open_browser=True
)

# This will open a browser window for you to log in and authorize the app.
# After you authorize, you will be redirected to the redirect URI.
# The URL of the redirected page will contain a 'code' parameter.
# Copy the entire URL and paste it into the terminal.

auth_manager.get_access_token(as_dict=False)

# The refresh token will be printed to the console.
# It will also be saved in a file named .cache in your current directory.

print(f"Refresh Token: {auth_manager.get_cached_token()['refresh_token']}")

```

Run this script, and it will guide you through the authorization process. At the end, it will print your refresh token. **Save this token securely.**

## Usage

Once you have your credentials, you can use the Spotify data source in PySpark.

```python
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("SpotifyExample").getOrCreate()

# Register the data source
from pyspark_datasources.spotify import SpotifyDataSource
spark.dataSource.register(SpotifyDataSource)

# Load your saved tracks
df = spark.read.format("spotify") \
    .option("spotify.client.id", "YOUR_CLIENT_ID") \
    .option("spotify.client.secret", "YOUR_CLIENT_SECRET") \
    .option("spotify.refresh.token", "YOUR_REFRESH_TOKEN") \
    .option("type", "tracks") \
    .load()

# Show the data
df.show()
```

## Schema

The schema for the `tracks` type is as follows:

| Field       | Type                |
|-------------|---------------------|
| id          | `string`            |
| name        | `string`            |
| artists     | `array<string>`     |
| album       | `string`            |
| duration_ms | `long`              |
| popularity  | `integer`           |
| added_at    | `string`            |


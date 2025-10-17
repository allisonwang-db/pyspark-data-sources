
from pyspark.sql import SparkSession

# This is an example of how to use the Spotify data source.
# Before running this, make sure you have followed the authentication
# instructions in docs/datasources/spotify.md to get your credentials.

# Create a SparkSession
spark = SparkSession.builder.appName("SpotifyExample").getOrCreate()

# Register the data source
from pyspark_datasources.spotify import SpotifyDataSource
spark.dataSource.register(SpotifyDataSource)

# Replace with your actual credentials
CLIENT_ID = "YOUR_CLIENT_ID"
CLIENT_SECRET = "YOUR_CLIENT_SECRET"
REFRESH_TOKEN = "YOUR_REFRESH_TOKEN"

# Load your saved tracks
try:
    df = (
        spark.read.format("spotify")
        .option("spotify.client.id", CLIENT_ID)
        .option("spotify.client.secret", CLIENT_SECRET)
        .option("spotify.refresh.token", REFRESH_TOKEN)
        .option("type", "tracks")
        .load()
    )

    # Show the data
    df.show()

    # Print the schema
    df.printSchema()

except Exception as e:
    print(f"An error occurred: {e}")
    print("Please ensure you have replaced the placeholder credentials with your actual credentials.")


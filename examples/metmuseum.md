# Met Museum API Data Source

Read artwork metadata from Metropolitan Museum of Art API. No credentials. Option `limit` (default 50, max 100).

```python
spark.dataSource.register(MetMuseumDataSource)
df = spark.read.format("metmuseum").option("limit", "20").load()
df.select("objectID", "title", "artistDisplayName", "department").show(truncate=40)
```

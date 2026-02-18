# Lorem Picsum API Data Source

Read placeholder photo metadata from picsum.photos. No credentials. Option `limit` (default 30, max 100).

```python
spark.dataSource.register(PicsumDataSource)
df = spark.read.format("picsum").load()
df.select("id", "author", "width", "height").show()
```

# Ghibli API Data Source

Read Studio Ghibli film catalog from ghibliapi.vercel.app. No credentials required.

```python
spark.dataSource.register(GhibliDataSource)
df = spark.read.format("ghibli").load()
df.select("title", "director", "release_date").show()
```

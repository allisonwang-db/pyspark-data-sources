# Agify API Data Source

Age prediction from first name via api.agify.io. No credentials. Option `path` = name (default `Michael`).

```python
spark.dataSource.register(AgifyDataSource)
df = spark.read.format("agify").option("path", "Emma").load()
df.show()
```

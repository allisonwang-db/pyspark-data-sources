# Advice Slip API Data Source

Read random advice from api.adviceslip.com. No credentials. Option `limit` (default 5, max 30).

```python
spark.dataSource.register(AdviceDataSource)
df = spark.read.format("advice").option("limit", "5").load()
df.show(truncate=False)
```

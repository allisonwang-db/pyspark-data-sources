# Quotable API Data Source

Read quotes from api.quotable.io. No auth required.

```python
spark.dataSource.register(QuotableDataSource)
df = spark.read.format("quotable").option("limit", "20").load()
df.show()
```

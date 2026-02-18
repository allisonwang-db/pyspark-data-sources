# HTTPBin Data Source

Read test data from httpbin.org. No auth required.

```python
spark.dataSource.register(HttpbinDataSource)
df = spark.read.format("httpbin").load()
df.show()
```

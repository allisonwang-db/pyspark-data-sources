# Bored API Data Source

Read activity suggestions from boredapi.com. No auth required.

```python
spark.dataSource.register(BoredDataSource)
df = spark.read.format("bored").option("limit", "10").load()
df.show()
```

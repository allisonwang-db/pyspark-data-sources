# Fake Store API Data Source

Read e-commerce test data from fakestoreapi.com. No credentials. Option `endpoint` (default `products`; can use `carts`, `users`, etc.).

```python
spark.dataSource.register(FakeStoreDataSource)
df = spark.read.format("fakestore").load()
df.select("id", "title", "price", "category").show()
```

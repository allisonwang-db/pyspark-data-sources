# ReqRes API Data Source

Read test users from reqres.in. No credentials. Option `page` (default 1).

```python
spark.dataSource.register(ReqresDataSource)
df = spark.read.format("reqres").load()
df.select("id", "email", "first_name", "last_name").show()
```

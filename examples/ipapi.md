# IP-API Data Source

IP geolocation from ip-api.com. No auth required.

```python
spark.dataSource.register(IpApiDataSource)
df = spark.read.format("ipapi").load()
df = spark.read.format("ipapi").load("8.8.8.8")
df.show()
```

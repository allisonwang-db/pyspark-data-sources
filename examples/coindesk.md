# CoinDesk API Data Source

Read current Bitcoin price (USD) from api.coindesk.com. No credentials.

```python
spark.dataSource.register(CoinDeskDataSource)
df = spark.read.format("coindesk").load()
df.select("code", "rate", "rate_float", "updated").show()
```

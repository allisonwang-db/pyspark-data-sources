# Pokemon API Data Source

Read Pokemon from PokeAPI. No credentials required. Option `limit` (default 50, max 200).

```python
spark.dataSource.register(PokemonDataSource)
df = spark.read.format("pokemon").option("limit", "30").load()
df.select("id", "name").show()
```

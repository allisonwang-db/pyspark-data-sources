# Cocktail DB Data Source

Read cocktail recipes from TheCocktailDB. No auth required.

```python
spark.dataSource.register(CocktailDataSource)
df = spark.read.format("cocktail").load("margarita")
df.show()
```

# Dog API Data Source

Read dog breed data from dog.ceo API. No credentials required.

```python
spark.dataSource.register(DogDataSource)
df = spark.read.format("dog").load()
df.show()
```

# Universities API Data Source

Read university data from universities.hipolabs.com. No auth required.

```python
spark.dataSource.register(UniversitiesDataSource)
df = spark.read.format("universities").load()
df = spark.read.format("universities").load("United States")
df.show()
```

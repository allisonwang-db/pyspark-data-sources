# TOML File Data Source

Read TOML files. Requires: `pip install pyspark-data-sources[toml]`

```python
spark.dataSource.register(TomlFileDataSource)
df = spark.read.format("tomlfile").load("/path/to/config.toml")
df.show()
```

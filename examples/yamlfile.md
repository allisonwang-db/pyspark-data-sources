# YAML File Data Source

Read YAML files. Requires: `pip install pyspark-data-sources[yaml]`

```python
spark.dataSource.register(YamlFileDataSource)
df = spark.read.format("yamlfile").load("/path/to/config.yaml")
df.show()
```

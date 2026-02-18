# JSON Lines Data Source

Read .jsonl files (one JSON object per line).

```python
spark.dataSource.register(JsonLinesDataSource)
df = spark.read.format("jsonlines").load("/path/to/file.jsonl")
df.show()
```

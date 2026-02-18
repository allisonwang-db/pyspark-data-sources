# Open Trivia Database Data Source

Read trivia questions from opentdb.com. No credentials. Option `amount` (default 10, max 50).

```python
spark.dataSource.register(TriviaDataSource)
df = spark.read.format("trivia").option("amount", "15").load()
df.select("category", "difficulty", "question").show(truncate=40)
```

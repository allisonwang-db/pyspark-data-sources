# Pokemon API Data Source Example

Read Pokemon from PokeAPI (pokeapi.co). No credentials required.

## Setup Credentials

None required.

## End-to-End Pipeline: Batch Read

### Step 1: Create Spark Session and Register Data Source

```python
from pyspark.sql import SparkSession
from pyspark_datasources import PokemonDataSource

spark = SparkSession.builder.appName("pokemon-example").getOrCreate()
spark.dataSource.register(PokemonDataSource)
```

### Step 2: Read Pokemon (Option: limit, default 50, max 200)

```python
df = spark.read.format("pokemon").option("limit", "30").load()
df.select("id", "name", "url").show(10, truncate=False)
```

### Example Output

```
+---+--------+------------------------------------------+
|id |name    |url                                       |
+---+--------+------------------------------------------+
|1  |bulbasaur|https://pokeapi.co/api/v2/pokemon/1/     |
|2  |ivysaur |https://pokeapi.co/api/v2/pokemon/2/      |
+---+--------+------------------------------------------+
```

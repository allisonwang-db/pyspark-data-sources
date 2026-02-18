# Data Sources Guide

This guide covers usage patterns and troubleshooting for the PySpark Data Sources library. For copy-pastable, end-to-end examples for each data source, see the [examples folder](../examples/README.md).

## Per-Source Examples

Each data source has a standalone example with credential setup, Spark session, and full pipeline code. See the [examples index](../examples/README.md) for the full list and links to each file.

## Common Patterns

### Error Handling

Most data sources raise informative errors when required options are missing:

```python
# This will raise an error about missing API key
df = spark.read.format("stock").option("symbols", "AAPL").load()
# ValueError: api_key option is required for StockDataSource
```

### Schema Inference vs Specification

Some data sources support both automatic schema inference and explicit schema specification:

```python
# Automatic schema inference
df = spark.read.format("fake").load()

# Explicit schema specification
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
])

df = spark.read.format("fake").schema(schema).load()
```

### Partitioning for Performance

Many data sources automatically partition data for parallel processing:

```python
# HuggingFace automatically partitions large datasets
df = spark.read.format("huggingface").load("wikipedia")
df.rdd.getNumPartitions()

# You can control partitioning in some data sources
df = spark.read.format("fake").option("numPartitions", "8").load()
```

## Troubleshooting

### Missing Dependencies

```bash
# Install specific extras
pip install pyspark-data-sources[faker,huggingface,kaggle]
```

### API Rate Limits

- **GitHub**: 60 requests/hour (unauthenticated)
- **Alpha Vantage**: 5 requests/minute (free tier)
- Use caching or implement retry logic where applicable

### Network Issues

- Most data sources require internet access
- Check firewall/proxy settings
- Some sources support offline mode after initial cache (e.g. HuggingFace, Kaggle)

### Memory Issues with Large Datasets

- Use partitioning for large datasets
- Consider sampling: `df.sample(0.1)`
- Increase Spark executor memory

---

For more help, see the [Development Guide](../contributing/DEVELOPMENT.md) or open an issue on GitHub.

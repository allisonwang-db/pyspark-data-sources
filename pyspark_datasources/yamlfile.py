"""YAML file data source - reads YAML files."""

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class YamlFileDataSource(DataSource):
    """
    A DataSource for reading YAML files. Path = file path.
    Name: `yamlfile` Schema: `key string, value string`
    """

    @classmethod
    def name(cls):
        return "yamlfile"

    def schema(self):
        return "key string, value string"

    def reader(self, schema):
        return YamlFileReader(self.options)


class YamlFileReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.path = options.get("path")
        if not self.path:
            raise ValueError("path is required for YamlFileDataSource")

    def read(self, partition):
        try:
            import yaml

            with open(self.path) as f:
                data = yaml.safe_load(f)
            if data is None:
                return iter([])
            for k, v in self._flatten(data, ""):
                yield Row(key=k, value=str(v)[:1000])
        except ImportError:
            raise ImportError("PyYAML required: pip install pyyaml")
        except (OSError, TypeError):
            return iter([])

    def _flatten(self, d, prefix):
        if isinstance(d, dict):
            for k, v in d.items():
                yield from self._flatten(v, f"{prefix}.{k}" if prefix else k)
        elif isinstance(d, list):
            for i, v in enumerate(d):
                yield from self._flatten(v, f"{prefix}[{i}]")
        else:
            yield (prefix, d)

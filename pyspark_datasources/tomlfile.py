"""TOML file data source - reads TOML config files."""

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class TomlFileDataSource(DataSource):
    """
    A DataSource for reading TOML files. Path = file path.
    Name: `tomlfile` Schema: `key string, value string`
    """

    @classmethod
    def name(cls):
        return "tomlfile"

    def schema(self):
        return "key string, value string"

    def reader(self, schema):
        return TomlFileReader(self.options)


class TomlFileReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.path = options.get("path")
        if not self.path:
            raise ValueError("path is required for TomlFileDataSource")

    def read(self, partition):
        try:
            import tomllib
        except ImportError:
            try:
                import tomli as tomllib
            except ImportError:
                raise ImportError("tomli required: pip install pyspark-data-sources[toml]")
        try:
            with open(self.path, "rb") as f:
                data = tomllib.load(f)
            for k, v in self._flatten(data, ""):
                yield Row(key=k, value=str(v)[:1000])
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

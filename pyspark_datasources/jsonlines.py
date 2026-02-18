"""JSON Lines data source - reads .jsonl files (one JSON object per line)."""

import json

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class JsonLinesDataSource(DataSource):
    """
    A DataSource for reading JSON Lines files (.jsonl).
    Path = file or directory. Schema: key-value pairs from first record.
    Name: `jsonlines`
    """

    @classmethod
    def name(cls):
        return "jsonlines"

    def schema(self):
        return "col_1 string, col_2 string, col_3 string, col_4 string, col_5 string"

    def reader(self, schema):
        return JsonLinesReader(self.options)


class JsonLinesReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.path = options.get("path")
        if not self.path:
            raise ValueError("path is required for JsonLinesDataSource")

    def read(self, partition):
        try:
            with open(self.path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            obj = json.loads(line)
                            if isinstance(obj, dict):
                                vals = [str(v)[:500] for _, v in list(obj.items())[:5]]
                                while len(vals) < 5:
                                    vals.append("")
                                yield Row(
                                    col_1=vals[0],
                                    col_2=vals[1],
                                    col_3=vals[2],
                                    col_4=vals[3],
                                    col_5=vals[4],
                                )
                            else:
                                yield Row(col_1=line[:500], col_2="", col_3="", col_4="", col_5="")
                        except json.JSONDecodeError:
                            yield Row(col_1=line[:500], col_2="", col_3="", col_4="", col_5="")
        except OSError:
            return iter([])

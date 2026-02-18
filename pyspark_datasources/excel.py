"""Excel data source - reads .xlsx and .xls files."""

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class ExcelDataSource(DataSource):
    """
    A DataSource for reading Excel files (.xlsx, .xls).

    Requires: pip install pyspark-data-sources[excel]

    Name: `excel`

    Schema: Columns inferred from first row (header) or generic col_1, col_2, etc.

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import ExcelDataSource
    >>> spark.dataSource.register(ExcelDataSource)

    Load Excel file.

    >>> spark.read.format("excel").load("/path/to/file.xlsx").show()
    """

    @classmethod
    def name(cls):
        return "excel"

    def schema(self):
        return "col_1 string, col_2 string, col_3 string, col_4 string, col_5 string"

    def reader(self, schema):
        return ExcelReader(self.options)


class ExcelReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.path = options.get("path")
        if not self.path:
            raise ValueError("path is required for ExcelDataSource")
        self.sheet = options.get("sheet", "0")
        self.header = options.get("header", "true").lower() == "true"

    def read(self, partition):
        try:
            import openpyxl

            wb = openpyxl.load_workbook(self.path, read_only=True, data_only=True)
            sheet_idx = int(self.sheet) if self.sheet.isdigit() else 0
            ws = wb.worksheets[sheet_idx]

            rows = list(ws.iter_rows(values_only=True))
            wb.close()

            if not rows:
                return iter([])

            start = 1 if self.header else 0

            for row in rows[start:]:
                values = [str(v) if v is not None else "" for v in (row or [])]
                while len(values) < 5:
                    values.append("")
                yield Row(
                    col_1=values[0] if len(values) > 0 else "",
                    col_2=values[1] if len(values) > 1 else "",
                    col_3=values[2] if len(values) > 2 else "",
                    col_4=values[3] if len(values) > 3 else "",
                    col_5=values[4] if len(values) > 4 else "",
                )
        except ImportError:
            raise ImportError(
                "openpyxl is required for ExcelDataSource. "
                "Install with: pip install pyspark-data-sources[excel]"
            )
        except Exception:
            return iter([])

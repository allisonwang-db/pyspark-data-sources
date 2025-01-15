from dataclasses import dataclass
from typing import Dict, Optional

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StringType, StructField, StructType


@dataclass
class Sheet:
    """
    A dataclass to identify a Google Sheets document.

    Attributes
    ----------
    spreadsheet_id : str
        The ID of the Google Sheets document.
    sheet_id : str, optional
        The ID of the worksheet within the document.
    """

    spreadsheet_id: str
    sheet_id: Optional[str]  # if None, the first sheet is used

    @classmethod
    def from_url(cls, url: str) -> "Sheet":
        """
        Converts a Google Sheets URL to a Sheet object.
        """
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(url)
        if parsed.netloc != "docs.google.com" or not parsed.path.startswith(
            "/spreadsheets/d/"
        ):
            raise ValueError("URL is not a Google Sheets URL")
        qs = parse_qs(parsed.query)
        spreadsheet_id = parsed.path.split("/")[3]
        if "gid" in qs:
            sheet_id = qs["gid"][0]
        else:
            sheet_id = None
        return cls(spreadsheet_id, sheet_id)

    def get_query_url(self, query: str = None):
        """
        Gets the query url that returns the results of the query as a CSV file.

        If no query is provided, returns the entire sheet.
        If sheet ID is None, uses the first sheet.

        See https://developers.google.com/chart/interactive/docs/querylanguage
        """
        from urllib.parse import urlencode

        path = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/gviz/tq"
        url_query = {"tqx": "out:csv"}
        if self.sheet_id:
            url_query["gid"] = self.sheet_id
        if query:
            url_query["tq"] = query
        return f"{path}?{urlencode(url_query)}"


class GoogleSheetsDataSource(DataSource):
    """
    A DataSource for reading table from Google Sheets.

    Name: `googlesheets`

    Schema: The first row of the sheet defines the column names. All columns are treated as strings.

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import GoogleSheetsDataSource
    >>> spark.dataSource.register(GoogleSheetsDataSource)

    Load data from a public Google Sheets document.

    >>> spark.read.format("googlesheets").options(url="https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=0#gid=0")
    +-------+----------+-----------+--------------------+
    |country|  latitude|  longitude|                name|
    +-------+----------+-----------+--------------------+
    |     AD| 42.546245|   1.601554|             Andorra|
    |    ...|       ...|        ...|                 ...|
    +-------+----------+-----------+--------------------+

    Options:
    - 'url': The URL of the Google Sheets document.
    - 'spreadsheet_id': The ID of the Google Sheets document.
    - 'sheet_id': The ID of the worksheet within the document.
    """

    @classmethod
    def name(self):
        return "googlesheets"

    def __init__(self, options: Dict[str, str]):
        import pandas as pd

        if "url" in options:
            self.sheet = Sheet.from_url(options["url"])
        elif "spreadsheet_id" in options:
            self.sheet = Sheet(options["spreadsheet_id"], options.get("sheet_id"))
        else:
            raise ValueError(
                "You must specify a URL or spreadsheet_id in `.options()`."
            )

        # Infer schema from the first row
        df = pd.read_csv(self.sheet.get_query_url("select * limit 0"))
        self.inferred_schema = StructType(
            [StructField(col, StringType()) for col in df.columns]
        )

    def schema(self) -> StructType:
        return self.inferred_schema

    def reader(self, schema: StructType) -> DataSourceReader:
        return GoogleSheetsReader(self.sheet, schema)


class GoogleSheetsReader(DataSourceReader):
    def __init__(self, sheet: Sheet, schema: StructType):
        self.sheet = sheet
        self.schema = schema

    def read(self, partition):
        from urllib.request import urlopen

        import pyarrow as pa
        from pyarrow import csv

        convert_options = csv.ConvertOptions(
            column_types={name: pa.string() for name in self.schema.fieldNames()}
        )
        with urlopen(self.sheet.get_query_url()) as file:
            yield from csv.read_csv(file, convert_options=convert_options).to_batches()

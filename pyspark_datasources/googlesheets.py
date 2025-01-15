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
    A DataSource for reading table from public Google Sheets.

    Name: `googlesheets`

    Schema: By default, all columns are treated as strings and the header row defines the column names.

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import GoogleSheetsDataSource
    >>> spark.dataSource.register(GoogleSheetsDataSource)

    Load data from a public Google Sheets document using url.

    >>> url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=0#gid=0"
    >>> spark.read.format("googlesheets").options(url=url)
    +-------+----------+-----------+--------------------+
    |country|  latitude|  longitude|                name|
    +-------+----------+-----------+--------------------+
    |     AD| 42.546245|   1.601554|             Andorra|
    |    ...|       ...|        ...|                 ...|
    +-------+----------+-----------+--------------------+

    Load data using `spreadsheet_id` and optional `sheet_id`.

    >>> spark.read.format("googlesheets").options(spreadsheet_id="10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0", sheet_id="0")
    +-------+----------+-----------+--------------------+
    |country|  latitude|  longitude|                name|
    +-------+----------+-----------+--------------------+
    |     AD| 42.546245|   1.601554|             Andorra|
    |    ...|       ...|        ...|                 ...|
    +-------+----------+-----------+--------------------+

    Specify custom schema.

    >>> spark.read.format("googlesheets").options(url=url).schema("id string, lat double, long double, name string")
    +---+----------+-----------+--------------------+
    | id|       lat|       long|                name|
    +---+----------+-----------+--------------------+
    | AD| 42.546245|   1.601554|             Andorra|
    |...|       ...|        ...|                 ...|
    +---+----------+-----------+--------------------+
    """

    @classmethod
    def name(self):
        return "googlesheets"

    def __init__(self, options: Dict[str, str]):
        if "url" in options:
            self.sheet = Sheet.from_url(options["url"])
        elif "spreadsheet_id" in options:
            self.sheet = Sheet(options["spreadsheet_id"], options.get("sheet_id"))
        else:
            raise ValueError(
                "You must specify a URL or spreadsheet_id in `.options()`."
            )

    def schema(self) -> StructType:
        import pandas as pd

        # Read schema from the first row of the sheet
        df = pd.read_csv(self.sheet.get_query_url("select * limit 0"))
        return StructType([StructField(col, StringType()) for col in df.columns])

    def reader(self, schema: StructType) -> DataSourceReader:
        return GoogleSheetsReader(self.sheet, schema)


class GoogleSheetsReader(DataSourceReader):
    def __init__(self, sheet: Sheet, schema: StructType):
        self.sheet = sheet
        self.schema = schema

    def read(self, partition):
        from urllib.request import urlopen

        from pyarrow import csv
        from pyspark.sql.pandas.types import to_arrow_type

        # Specify column types based on the schema
        convert_options = csv.ConvertOptions(
            column_types={
                field.name: to_arrow_type(field.dataType) for field in self.schema
            },
        )
        read_options = csv.ReadOptions(
            column_names=self.schema.fieldNames(),  # Rename columns
            skip_rows=1,  # Skip the header row
        )
        with urlopen(self.sheet.get_query_url()) as file:
            yield from csv.read_csv(
                file, read_options=read_options, convert_options=convert_options
            ).to_batches()

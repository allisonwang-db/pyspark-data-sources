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
    sheet_id: Optional[str] = None  # if None, the first sheet is used

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

    def get_query_url(self, query: Optional[str] = None):
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


@dataclass
class Parameters:
    sheet: Sheet
    has_header: bool


class GoogleSheetsDataSource(DataSource):
    """
    A DataSource for reading table from public Google Sheets.

    Name: `googlesheets`

    Schema: By default, all columns are treated as strings and the header row defines the column names.

    Options
    --------
    - `url`: The URL of the Google Sheets document.
    - `path`: The ID of the Google Sheets document.
    - `sheet_id`: The ID of the worksheet within the document.
    - `has_header`: Whether the sheet has a header row. Default is `true`.

    Either `url` or `path` must be specified, but not both.

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import GoogleSheetsDataSource
    >>> spark.dataSource.register(GoogleSheetsDataSource)

    Load data from a public Google Sheets document using `path` and optional `sheet_id`.

    >>> spreadsheet_id = "10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0"
    >>> spark.read.format("googlesheets").options(sheet_id="0").load(spreadsheet_id).show()
    +-------+---------+---------+-------+
    |country| latitude|longitude|   name|
    +-------+---------+---------+-------+
    |     AD|42.546245| 1.601554|Andorra|
    |    ...|      ...|      ...|    ...|
    +-------+---------+---------+-------+

    Load data from a public Google Sheets document using `url`.

    >>> url = "https://docs.google.com/spreadsheets/d/10pD8oRN3RTBJq976RKPWHuxYy0Qa_JOoGFpsaS0Lop0/edit?gid=0#gid=0"
    >>> spark.read.format("googlesheets").options(url=url).load().show()
    +-------+---------+--------+-------+
    |country| latitude|ongitude|   name|
    +-------+---------+--------+-------+
    |     AD|42.546245|1.601554|Andorra|
    |    ...|      ...|     ...|    ...|
    +-------+---------+--------+-------+

    Specify custom schema.

    >>> schema = "id string, lat double, long double, name string"
    >>> spark.read.format("googlesheets").schema(schema).options(url=url).load().show()
    +---+---------+--------+-------+
    | id|      lat|    long|   name|
    +---+---------+--------+-------+
    | AD|42.546245|1.601554|Andorra|
    |...|      ...|     ...|    ...|
    +---+---------+--------+-------+

    Treat first row as data instead of header.

    >>> schema = "c1 string, c2 string, c3 string, c4 string"
    >>> spark.read.format("googlesheets").schema(schema).options(url=url, has_header="false").load().show()
    +-------+---------+---------+-------+
    |     c1|       c2|       c3|     c4|
    +-------+---------+---------+-------+
    |country| latitude|longitude|   name|
    |     AD|42.546245| 1.601554|Andorra|
    |    ...|      ...|      ...|    ...|
    +-------+---------+---------+-------+
    """

    @classmethod
    def name(self):
        return "googlesheets"

    def __init__(self, options: Dict[str, str]):
        if "url" in options:
            sheet = Sheet.from_url(options.pop("url"))
        elif "path" in options:
            sheet = Sheet(options.pop("path"), options.pop("sheet_id", None))
        else:
            raise ValueError(
                "You must specify either `url` or `path` (spreadsheet ID)."
            )
        has_header = options.pop("has_header", "true").lower() == "true"
        self.parameters = Parameters(sheet, has_header)

    def schema(self) -> StructType:
        if not self.parameters.has_header:
            raise ValueError("Custom schema is required when `has_header` is false")

        import pandas as pd

        # Read schema from the first row of the sheet
        df = pd.read_csv(self.parameters.sheet.get_query_url("select * limit 1"))
        return StructType([StructField(col, StringType()) for col in df.columns])

    def reader(self, schema: StructType) -> DataSourceReader:
        return GoogleSheetsReader(self.parameters, schema)


class GoogleSheetsReader(DataSourceReader):

    def __init__(self, parameters: Parameters, schema: StructType):
        self.parameters = parameters
        self.schema = schema

    def read(self, partition):
        from urllib.request import urlopen

        from pyarrow import csv
        from pyspark.sql.pandas.types import to_arrow_schema

        # Specify column types based on the schema
        convert_options = csv.ConvertOptions(
            column_types=to_arrow_schema(self.schema),
        )
        read_options = csv.ReadOptions(
            column_names=self.schema.fieldNames(),  # Rename columns
            skip_rows=(
                1 if self.parameters.has_header else 0  # Skip header row if present
            ),
        )
        with urlopen(self.parameters.sheet.get_query_url()) as file:
            yield from csv.read_csv(
                file, read_options=read_options, convert_options=convert_options
            ).to_batches()

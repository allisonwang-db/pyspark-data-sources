from dataclasses import dataclass
from typing import Dict, Any, List, Callable
import ultimate_notion as uno
from pyspark.sql.datasource import DataSource, DataSourceReader, DataSourceWriter
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    ArrayType,
)
import pyarrow as pa


def default_client_factory(api_key: str) -> uno.Session:
    return uno.Session(auth=api_key)


@dataclass
class Parameters:
    database_id: str
    api_key: str


class NotionDataSource(DataSource):
    @classmethod
    def name(self):
        return "notion"

    def __init__(self, options: Dict[str, str]):
        self.parameters = Parameters(
            database_id=options["database_id"],
            api_key=options["api_key"],
        )

    def schema(self) -> StructType:
        with default_client_factory(self.parameters.api_key) as notion:
            db = notion.get_db(self.parameters.database_id)
            properties = db.schema
            fields = []
            for prop in properties:
                prop_type = prop.type.name
                if prop_type == "TITLE":
                    fields.append(StructField(prop.name, StringType(), True))
                elif prop_type in ("RICH_TEXT", "TEXT"):
                    fields.append(StructField(prop.name, StringType(), True))
                elif prop_type == "NUMBER":
                    fields.append(StructField(prop.name, DoubleType(), True))
                elif prop_type == "CHECKBOX":
                    fields.append(StructField(prop.name, BooleanType(), True))
                elif prop_type == "DATE":
                    fields.append(StructField(prop.name, DateType(), True))
                elif prop_type == "CREATED_TIME":
                    fields.append(StructField(prop.name, TimestampType(), True))
                elif prop_type == "LAST_EDITED_TIME":
                    fields.append(StructField(prop.name, TimestampType(), True))
                elif prop_type == "MULTI_SELECT":
                    fields.append(StructField(prop.name, ArrayType(StringType()), True))
                else:
                    fields.append(StructField(prop.name, StringType(), True))
            return StructType(fields)

    def reader(self, schema: StructType) -> DataSourceReader:
        return NotionReader(self.parameters, schema, default_client_factory)

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return NotionWriter(self.parameters, schema, overwrite, default_client_factory)


class NotionReader(DataSourceReader):
    def __init__(self, parameters: Parameters, schema: StructType, client_factory: Callable[[str], uno.Session]):
        self.parameters = parameters
        self.schema = schema
        self.client_factory = client_factory

    def read(self, partition):
        with self.client_factory(self.parameters.api_key) as notion:
            db = notion.get_db(self.parameters.database_id)
            pages = db.pages
            if not pages:
                return

            data: Dict[str, List[Any]] = {field.name: [] for field in self.schema}
            for page in pages:
                for field in self.schema:
                    prop = getattr(page, field.name, None)
                    data[field.name].append(self._extract_property_value(prop))

            arrow_schema = to_arrow_schema(self.schema)
            arrow_table = pa.Table.from_pydict(data, schema=arrow_schema)
            yield from arrow_table.to_batches()

    def _extract_property_value(self, prop):
        if prop is None:
            return None
        if isinstance(prop, (uno.props.Title, uno.props.Text)):
            return str(prop.value)
        if isinstance(prop, (uno.props.Number, uno.props.Checkbox, uno.props.CreatedTime, uno.props.LastEditedTime)):
            return prop.value
        if isinstance(prop, uno.props.Date):
            return prop.value.start
        if isinstance(prop, uno.props.MultiSelect):
            return [opt.name for opt in prop.value]
        return None


class NotionWriter(DataSourceWriter):
    def __init__(self, parameters: Parameters, schema: StructType, overwrite: bool, client_factory: Callable[[str], uno.Session]):
        self.parameters = parameters
        self.schema = schema
        self.overwrite = overwrite
        self.client_factory = client_factory
        with self.client_factory(self.parameters.api_key) as notion:
            self.db_schema = notion.get_db(self.parameters.database_id).schema

    def write(self, messages):
        with self.client_factory(self.parameters.api_key) as notion:
            db = notion.get_db(self.parameters.database_id)
            if self.overwrite:
                for page in db.pages:
                    page.archive()
            for message in messages:
                data = message.to_pydict()
                num_rows = len(next(iter(data.values())))
                for i in range(num_rows):
                    properties = {}
                    for field in self.schema:
                        value = data[field.name][i]
                        properties[field.name] = self._format_property_value(field, value)
                    db.add_page(**properties)

    def _format_property_value(self, field: StructField, value: Any):
        field_type = str(field.dataType)
        if field_type == "StringType":
            if isinstance(self.db_schema[field.name], uno.schema.Title):
                 return uno.props.Title(value)
            return uno.props.Text(value)
        if field_type == "DoubleType":
            return uno.props.Number(value)
        if field_type == "BooleanType":
            return uno.props.Checkbox(value)
        if field_type == "DateType":
            return uno.props.Date(value)
        if field_type == "ArrayType(StringType,true)":
            return uno.props.MultiSelect([uno.props.SelectOption(name) for name in value])
        return None

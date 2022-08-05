from typing import Optional, Dict, Any, Union, TYPE_CHECKING
from pyspark.sql import DataFrameReader, DataFrameWriter, DataFrame
from py4j.java_gateway import JavaObject
from pyspark.sql.session import SparkSession
from jibaro.datalake.path import mount_path

if TYPE_CHECKING:
    from pyspark.sql.context import SQLContext
    from pyspark.sql._typing import OptionalPrimitiveType, ColumnOrName


class JibaroDataFrameWriter(DataFrameWriter):

    def __init__(self, df):
        super().__init__(df)

    def __getitem__(self, item):
        return super().__getitem__(item)

    def json(
        self, layer: str, project_name: str, database: str, table_name: str,
        mode=None, compression=None, dateFormat=None, timestampFormat=None,
        lineSep=None, encoding=None, ignoreNullFields=None
    ):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        super().json(path, mode=mode, compression=compression, dateFormat=dateFormat,
                     timestampFormat=timestampFormat, lineSep=lineSep, encoding=encoding,
                     ignoreNullFields=ignoreNullFields)

    def parquet(
        self, layer: str, project_name: str, database: str, table_name: str,
        mode=None, partitionBy=None, compression=None
    ) -> None:
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        super().parquet(path, mode=mode, partitionBy=partitionBy, compression=compression)

    def save(
        self, layer: str, project_name: str, database: str, table_name: str,
        path: str, format: str, mode: str = None,
        partitionBy: Optional[Union[str, "ColumnOrName"]] = None, **options
    ) -> None:
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        return super().save(path, format, mode=mode, partitionBy=partitionBy, **options)


class JibaroDataFrame(DataFrame):

    def __init__(self, jdf: JavaObject, sql_ctx: Union["SQLContext", "SparkSession"]):
        super().__init__(jdf, sql_ctx)

    @property
    def write(self) -> JibaroDataFrameWriter:
        return JibaroDataFrameWriter(self)


class JibaroDataFrameReader(DataFrameReader):

    def __init__(self, spark):
        super().__init__(spark)

    def __getitem__(self, item):
        return super().__getitem__(item)

    def load(self, layer: str, project_name: str, database: str, table_name: str,
             format: str, options: Dict[str, Any] = None) -> JibaroDataFrame:
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().load(path, format, options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def parquet(self, layer: str, project_name: str, database: str, table_name: str,
                **options: "OptionalPrimitiveType") -> "JibaroDataFrame":
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().parquet(*[path], **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def json(self, layer: str, project_name: str, database: str, table_name: str,
             path, schema=None, primitivesAsString=None, prefersDecimal=None,
             allowComments=None, allowUnquotedFieldNames=None, allowSingleQuotes=None,
             allowNumericLeadingZero=None, allowBackslashEscapingAnyCharacter=None,
             mode=None, columnNameOfCorruptRecord=None, dateFormat=None, timestampFormat=None,
             multiLine=None, allowUnquotedControlChars=None, lineSep=None, samplingRatio=None,
             dropFieldIfAllNull=None, encoding=None, locale=None, pathGlobFilter=None,
             recursiveFileLookup=None, allowNonNumericNumbers=None,
             modifiedBefore=None, modifiedAfter=None) -> "JibaroDataFrame":
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().json(
            path=path, schema=schema, primitivesAsString=primitivesAsString, prefersDecimal=prefersDecimal,
            allowComments=allowComments, allowUnquotedFieldNames=allowUnquotedFieldNames, allowSingleQuotes=allowSingleQuotes,
            allowNumericLeadingZero=allowNumericLeadingZero, allowBackslashEscapingAnyCharacter=allowBackslashEscapingAnyCharacter,
            mode=mode, columnNameOfCorruptRecord=columnNameOfCorruptRecord, dateFormat=dateFormat, timestampFormat=timestampFormat,
            multiLine=multiLine, allowUnquotedControlChars=allowUnquotedControlChars, lineSep=lineSep, samplingRatio=samplingRatio,
            dropFieldIfAllNull=dropFieldIfAllNull, encoding=encoding, locale=locale, pathGlobFilter=pathGlobFilter,
            recursiveFileLookup=recursiveFileLookup, allowNonNumericNumbers=allowNonNumericNumbers,
            modifiedBefore=modifiedBefore, modifiedAfter=modifiedAfter
        )

        dataframe.__class__ = JibaroDataFrame
        return dataframe

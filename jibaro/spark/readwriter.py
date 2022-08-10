from typing import Optional, Union, TYPE_CHECKING
from pyspark.sql import DataFrameReader, DataFrameWriter
from pyspark.sql.types import StructType
from jibaro.datalake.path import mount_path

if TYPE_CHECKING:
    from jibaro.spark.dataframe import JibaroDataFrame
    from pyspark.sql._typing import OptionalPrimitiveType


class JibaroDataFrameWriter(DataFrameWriter):

    def __init__(self, df):
        super().__init__(df)

    def __getitem__(self, item):
        return super().__getitem__(item)

    def json(
        self, layer: str, project_name: str, database: str, table_name: str,
        **options: "OptionalPrimitiveType"
    ):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        super().json(path, **options)

    def parquet(
        self, layer: str, project_name: str, database: str, table_name: str,
        **options: "OptionalPrimitiveType"
    ):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        super().parquet(path, **options)

    def text(
        self, layer: str, project_name: str, database: str, table_name: str,
        **options: "OptionalPrimitiveType"
    ):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        super().text(path, **options)

    def orc(
        self, layer: str, project_name: str, database: str, table_name: str,
        **options: "OptionalPrimitiveType"
    ):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        super().orc(path, **options)

    def save(
        self, layer: str, project_name: str, database: str, table_name: str,
        **options: "OptionalPrimitiveType"
    ) -> None:
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        super().save(path, **options)


class JibaroDataFrameReader(DataFrameReader):

    def __init__(self, spark):
        super().__init__(spark)

    def __getitem__(self, item):
        return super().__getitem__(item)

    def load(self, layer: str, project_name: str, database: str, table_name: str,
             **options: "OptionalPrimitiveType") -> "JibaroDataFrame":
        from jibaro.spark.dataframe import JibaroDataFrame
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().load(path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def parquet(self, layer: str, project_name: str, database: str, table_name: str,
                **options: "OptionalPrimitiveType") -> "JibaroDataFrame":
        from jibaro.spark.dataframe import JibaroDataFrame
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().parquet(*[path], **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def json(self, layer: str, project_name: str, database: str, table_name: str,
             **options: "OptionalPrimitiveType") -> "JibaroDataFrame":
        from jibaro.spark.dataframe import JibaroDataFrame
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().json(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def text(self, layer: str, project_name: str, database: str, table_name: str,
             **options: "OptionalPrimitiveType"):
        from jibaro.spark.dataframe import JibaroDataFrame
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().text(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def orc(self, layer: str, project_name: str, database: str, table_name: str,
            **options: "OptionalPrimitiveType"):
        from jibaro.spark.dataframe import JibaroDataFrame
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().orc(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

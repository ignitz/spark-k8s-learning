
from typing import TYPE_CHECKING
from pyspark.sql import DataFrameReader, DataFrameWriter
from jibaro.datalake.path import mount_path


if TYPE_CHECKING:
    from jibaro.spark.dataframe import JibaroDataFrame
    from pyspark.sql._typing import OptionalPrimitiveType

__all__ = ['JibaroDataFrameReader', 'JibaroDataFrameWriter']


class JibaroDataFrameWriter(DataFrameWriter):

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

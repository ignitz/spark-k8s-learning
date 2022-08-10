from typing import Optional
from pyspark.sql.streaming import DataStreamReader, DataStreamWriter
from jibaro.datalake.path import mount_path, mount_checkpoint_path
from jibaro.spark.dataframe import JibaroDataFrame

__all__ = ['JibaroDataStreamReader', 'JibaroDataStreamWriter']


class JibaroDataStreamReader(DataStreamReader):

    def load(self, layer: Optional[str] = None, project_name: Optional[str] = None, database: Optional[str] = None, table_name: Optional[str] = None, **options: dict):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().load(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def json(self, layer: str, project_name: str, database: str, table_name: str, **options: dict):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().json(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def orc(self, layer: str, project_name: str, database: str, table_name: str, **options: dict):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().orc(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def parquet(self, layer: str, project_name: str, database: str, table_name: str, **options: dict):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().parquet(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def text(self, layer: str, project_name: str, database: str, table_name: str, **options: dict):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().text(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe

    def csv(self, layer: str, project_name: str, database: str, table_name: str, **options: dict):
        path: str = mount_path(
            layer=layer, project_name=project_name, database=database, table_name=table_name)
        dataframe = super().csv(path=path, **options)
        dataframe.__class__ = JibaroDataFrame
        return dataframe


class JibaroDataStreamWriter(DataStreamWriter):

    def start(self, layer: Optional[str] = None, project_name: Optional[str] = None, database: Optional[str] = None, table_name: Optional[str] = None, **options: dict):
        if all([layer, project_name, database, table_name]) == False:
            return super().start(**options)
        else:
            if options.get('checkpointLocation') is None:
                options['checkpointLocation'] = mount_checkpoint_path(
                    layer=layer, project_name=project_name, database=database, table_name=table_name)
            path: str = mount_path(
                layer=layer, project_name=project_name, database=database, table_name=table_name)
            return super().start(path=path, **options)

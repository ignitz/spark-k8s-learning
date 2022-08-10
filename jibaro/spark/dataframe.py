
from pyspark.sql import DataFrame

__all__ = ['JibaroDataFrame']


class JibaroDataFrame(DataFrame):

    @property
    def write(self):
        from jibaro.spark.readwriter import JibaroDataFrameWriter
        return JibaroDataFrameWriter(self)

    @property
    def writeStream(self):
        from jibaro.spark.streaming import JibaroDataStreamWriter
        return JibaroDataStreamWriter(self)

from typing import Union, TYPE_CHECKING
from pyspark.sql import DataFrame
from py4j.java_gateway import JavaObject

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.context import SQLContext


class JibaroDataFrame(DataFrame):

    @property
    def write(self):
        from jibaro.spark.readwriter import JibaroDataFrameWriter
        return JibaroDataFrameWriter(self)

    @property
    def writeStream(self):
        from jibaro.spark.streaming import JibaroDataStreamWriter
        return JibaroDataStreamWriter(self)

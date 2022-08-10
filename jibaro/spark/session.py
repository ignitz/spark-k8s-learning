from pyspark.sql import SparkSession
# from typing import Optional, Dict, Any, TYPE_CHECKING, overload
from py4j.java_gateway import JavaObject
from pyspark import SparkContext
from pyspark.sql.session import SparkSession


class JibaroSession(SparkSession):

    class JibaroBuilder(SparkSession.Builder):
        def __init__(self):
            super().__init__()

        def getOrCreate(self) -> "JibaroSession":
            with self._lock:
                from pyspark.context import SparkContext
                from pyspark.conf import SparkConf
                session = JibaroSession._instantiatedSession
                if session is None or session._sc._jsc is None:
                    if self._sc is not None:
                        sc = self._sc
                    else:
                        sparkConf = SparkConf()
                        for key, value in self._options.items():
                            sparkConf.set(key, value)
                        # This SparkContext may be an existing one.
                        sc = SparkContext.getOrCreate(sparkConf)
                    # Do not update `SparkConf` for existing `SparkContext`, as it's shared
                    # by all sessions.
                    session = JibaroSession(sc)
                for key, value in self._options.items():
                    session._jsparkSession.sessionState().conf().setConfString(key, value)
                return session

    builder = JibaroBuilder()

    def __init__(self, spark_context: SparkContext, jsession: JavaObject = None):
        super().__init__(sparkContext=spark_context,
                         jsparkSession=jsession)

    @property
    def read(self):
        from jibaro.spark.readwriter import JibaroDataFrameReader
        return JibaroDataFrameReader(self._wrapped)

    @property
    def readStream(self):
        from jibaro.spark.streaming import JibaroDataStreamReader
        return JibaroDataStreamReader(self._wrapped)

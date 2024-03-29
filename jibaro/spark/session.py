
from pyspark.sql import SparkSession

__all__ = ['JibaroSparkSession']


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

    @property
    def read(self):
        from jibaro.spark.readwriter import JibaroDataFrameReader
        if hasattr(self, '_wrapped'):
            return JibaroDataFrameReader(self._wrapped)
        else:
            return JibaroDataFrameReader(self)

    @property
    def readStream(self):
        from jibaro.spark.streaming import JibaroDataStreamReader
        if hasattr(self, '_wrapped'):
            return JibaroDataStreamReader(self._wrapped)
        else:
            return JibaroDataStreamReader(self)

from jibaro.spark.session import JibaroSession
from jibaro.datalake.cdc import kafka_to_raw
from jibaro.settings import settings
import sys

topic = sys.argv[1]

spark = JibaroSession.builder.appName("Spark Streaming Delta").getOrCreate()
spark.__class__ = JibaroSession

kafka_settings = settings.kafka_settings['local']

kafka_to_raw(
    spark=spark,
    output_layer='raw',
    bootstrap_servers=kafka_settings['bootstrap_servers'],
    topic=topic
)

# For Spark 3.1.x
spark.stop()

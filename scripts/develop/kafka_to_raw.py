from pyspark.sql import SparkSession

from jibaro.datalake.cdc import kafka_to_raw
from jibaro.settings import settings
import sys

topic = sys.argv[1]

spark = SparkSession.builder.appName("Spark Streaming Delta").getOrCreate()

kafka_settings = settings.kafka_settings['example']

kafka_to_raw(
    spark=spark,
    database='kafka',
    bootstrap_servers=kafka_settings['bootstrap_servers'],
    topic=topic
)

# For Spark 3.1.x
spark.stop()

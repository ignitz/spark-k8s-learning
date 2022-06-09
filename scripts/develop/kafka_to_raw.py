from pyspark.sql import SparkSession

from jibaro.kafka import kafka_to_raw
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

# import time
# time.sleep(10000.0)
spark.stop()
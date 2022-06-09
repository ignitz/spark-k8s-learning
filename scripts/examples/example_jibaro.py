from jibaro.kafka import kafka_to_raw
from pyspark.sql import SparkSession
import sys

topic = sys.argv[1]

spark = SparkSession.builder.appName("Spark Streaming Delta").getOrCreate()

kafka_to_raw(
    spark,
    'kafka',
    'broker:29092',
    topic
)

# import time
# time.sleep(10000.0)
spark.stop()
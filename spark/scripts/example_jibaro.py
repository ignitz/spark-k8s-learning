from jibaro import kafka
from pyspark.sql import SparkSession
import sys

topic = sys.argv[1]

spark = SparkSession.builder.appName("Spark Streaming Delta").getOrCreate()

kafka.kafka_to_raw(
    spark,
    'kafka',
    'broker:29092',
    topic
)

spark.stop()
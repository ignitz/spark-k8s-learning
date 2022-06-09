from pyspark.sql import SparkSession

from jibaro.delta_handler import raw_to_staged
import sys

table_name = sys.argv[1]

spark = SparkSession.builder.appName("Spark Streaming Delta").getOrCreate()


raw_to_staged(
    spark=spark,
    database='kafka',
    table_name=table_name
)

# import time
# time.sleep(10000.0)
spark.stop()
from pyspark.sql import SparkSession

from jibaro.datalake.cdc import raw_to_staged
import sys

table_name = sys.argv[1]

spark = SparkSession.builder.appName("Spark Streaming Delta - raw to staged").getOrCreate()

raw_to_staged(
    spark=spark,
    database='kafka',
    table_name=table_name
)

spark.stop()
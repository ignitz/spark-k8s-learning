from pyspark.sql import SparkSession

from jibaro.datalake.cdc import staged_to_curated
import sys

table_name = sys.argv[1]

spark = SparkSession.builder.appName(
    "Spark Streaming Delta - raw to staged").getOrCreate()

staged_to_curated(
    spark=spark,
    database='kafka',
    table_name=table_name,
    environment='example'
)

# For Spark 3.1.x
spark.stop()

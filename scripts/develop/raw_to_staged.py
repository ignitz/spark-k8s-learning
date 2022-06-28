from pyspark.sql import SparkSession

from jibaro.datalake.cdc import raw_to_staged
import sys

table_name = sys.argv[1]

content_type = sys.argv[2] if len(sys.argv) > 2 else 'avro'

spark = SparkSession.builder.appName("Raw to Staged").getOrCreate()

raw_to_staged(
    spark=spark,
    database='kafka',
    table_name=table_name,
    environment='example',
    content_type=content_type
)

# For Spark 3.1.x
spark.stop()

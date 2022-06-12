from pyspark.sql import SparkSession

from jibaro.datalake.cdc import kafka_to_raw, raw_to_staged

from jibaro.settings import settings
import sys

table_name = 'dbserver1.inventory.products'
content_type = 'json'

# table_name = 'airflow.public.ab_user'
# content_type = 'avro'

spark = SparkSession.builder.appName("Spark Streaming Delta - raw to staged").getOrCreate()

# kafka_settings = settings.kafka_settings['local']

# kafka_to_raw(
#     spark=spark,
#     database='kafka',
#     bootstrap_servers=kafka_settings['bootstrap_servers'],
#     topic=table_name
# )

raw_to_staged(
    spark=spark,
    database='kafka',
    table_name=table_name,
    environment='local',
    content_type=content_type
)

spark.stop()


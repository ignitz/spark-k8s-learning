# from pyspark.sql import SparkSession
from jibaro.spark.session import JibaroSession

from jibaro.datalake.cdc import raw_to_staged
import sys

project_name = sys.argv[1]
database = sys.argv[2]
table_name = sys.argv[3]

content_type = sys.argv[4] if len(sys.argv) > 4 else 'avro'

spark = JibaroSession.builder.appName("Raw to Staged").getOrCreate()

raw_to_staged(
    spark=spark,
    project_name=project_name,
    database=database,
    table_name=table_name,
    environment='local',
    content_type=content_type
)

# For Spark 3.1.x
spark.stop()

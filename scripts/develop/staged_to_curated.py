from jibaro.spark.session import JibaroSession

from jibaro.datalake.cdc import staged_to_curated
import sys

project_name = sys.argv[1]
database = sys.argv[2]
table_name = sys.argv[3]

spark = JibaroSession.builder.appName("Staged to Curated").getOrCreate()

staged_to_curated(
    spark=spark,
    project_name=project_name,
    database=database,
    table_name=table_name,
    environment='local'
)

# For Spark 3.1.x
spark.stop()

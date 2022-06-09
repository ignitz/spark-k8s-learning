# Coloca isso no ingress do minio
# annotations:
#   nginx.ingress.kubernetes.io/proxy-body-size: "0"
import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark SQL").getOrCreate()

data = requests.get('https://pokeapi.co/api/v2/pokemon').json()['results']

df = spark.createDataFrame(data)
df.write.format('delta').mode('overwrite').save('s3a://datalake-raw/pokeapi')

spark.stop()
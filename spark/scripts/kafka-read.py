from ensurepip import bootstrap
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Streaming Delta").getOrCreate()

topic = sys.argv[1]
bootstrap_servers = 'broker:29092'

df = (
    spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', bootstrap_servers)
    .option('subscribe', topic)
    .option('startingOffsets', 'earliest')
    .load()
)

# df.writeStream.trigger(once=True).format("console").start().awaitTermination()

df.writeStream.trigger(once=True).format("delta").start(
    path=f's3a://datalake/data/{topic}',
    checkpointLocation=f's3a://datalake/_checkpoints/kafka/{topic}'
).awaitTermination()

spark.stop()

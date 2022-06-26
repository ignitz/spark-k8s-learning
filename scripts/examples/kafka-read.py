import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Streaming Delta").getOrCreate()

topic = sys.argv[1]
bootstrap_servers = 'localhost:9092'

df = (
    spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', bootstrap_servers)
    .option('subscribe', topic)
    .option('startingOffsets', 'earliest')
    # Will work only with Spark 3.3 with
    .option('maxOffsetsPerTrigger', 100000)
    .load()
)

# availableNow only work with Spark 3.3.x or later
df.writeStream.trigger(availableNow=True).format(
    "console").start().awaitTermination()

# path = f'datalake-raw/data/{topic}'

# df.writeStream.trigger(once=True).format("delta").start(
#     path=f's3a://{path}',
#     checkpointLocation=f's3a://spark-control/_checkpoints/{path}'
# ).awaitTermination()

# spark.stop()

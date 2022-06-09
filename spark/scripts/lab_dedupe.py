import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, window, session_window

spark = SparkSession.builder.appName("Spark Streaming Delta").getOrCreate()

topic = "dbserver1.inventory.products"
bootstrap_servers = 'localhost:9092'

df = (
    spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', bootstrap_servers)
    .option('subscribe', topic)
    .option('startingOffsets', 'earliest')
    .load()
)

# CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081

# df.writeStream.trigger(once=True).format("console").start().awaitTermination()

# df.writeStream.trigger(once=True).format("delta").start(
#     path=f's3a://datalake/data/{topic}',
#     checkpointLocation=f's3a://datalake/_checkpoints/kafka/{topic}'
# ).awaitTermination()

(
    df.withWatermark("timestamp", "10 minutes").writeStream.trigger(once=True).format("delta").start(
        path=f's3a://datalake/data/{topic}',
        checkpointLocation=f's3a://datalake/_checkpoints/kafka/{topic}'
    ).awaitTermination()
)

(
    df
    .groupBy(
        window(df.timestamp, "10 minutes"),
        df.key) \
    .count()
).writeStream.trigger(once=True).format("console").start().awaitTermination()

(
    df.groupBy("key").count()
    .writeStream.trigger(once=True).format("console").start().awaitTermination()   
)
    # 

spark.read.format('delta').load(f's3a://datalake/data/{topic}').show()

# spark.stop()
from delta import DeltaTable

deltaTable = DeltaTable.forPath(spark, f's3a://datalake/data/{topic}')

def upsertToDelta(microBatchOutputDF, batchId):
    microBatchOutputDF = microBatchOutputDF.groupBy("key").agg(
        max("timestamp").alias("timestamp")
    ).join(
        microBatchOutputDF,
        on=["key", "timestamp"],
        how="inner"
    )
    print(batchId)
    microBatchOutputDF.selectExpr("cast(value as string)", "offset", "timestamp").show(truncate=False)
    return deltaTable.as("t") \
            .merge(
                microBatchOutputDF.as("s"), 
                "s.key = t.key"
            ) \
            .whenMatched().updateAll() \
            .whenNotMatched().insertAll() \
            .execute()

(
    df
    .writeStream.trigger(once=True).format("console").foreachBatch(upsertToDelta).start().awaitTermination()   
)

(
    df
    .writeStream.trigger(once=True).format("console").start().awaitTermination()   
)
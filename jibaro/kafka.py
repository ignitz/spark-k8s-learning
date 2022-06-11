from jibaro.settings import settings


def kafka_to_raw(spark, database, bootstrap_servers, topic):
    # TODO: make sure that only one instance of sparksession is running

    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_servers)
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        .option('maxOffsetsPerTrigger', 100) # Will work only with Spark 3.3 with 
        .load()
    )

    path: str = f'{database}/{topic}'
    output_path: str = f'{settings.prefix_protocol}://{settings.raw}/{path}'
    checkpoint_location: str = f'{settings.checkpoint_raw}/{path}'

    df.writeStream.trigger(once=True).format("delta").start(
        path=output_path,
        checkpointLocation=checkpoint_location
    ).awaitTermination()

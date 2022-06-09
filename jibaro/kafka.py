from jibaro.settings import settings

def kafka_to_raw(spark, database, bootstrap_servers, topic):
    # TODO: make sure that only one instance of sparksession is running

    # TODO: Limit batch size in 1 million rows
    # spark.streaming.backpressure.enabled=true
    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_servers)
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        .option('maxOffsetsPerTrigger', 1)
        .load()
    )

    path: str = f'{database}/{topic}'
    output_path: str = f'{settings.prefix_protocol}://{settings.raw}/{path}'
    checkpoint_location: str = f'{settings.checkpoint_raw}/{path}'


    df.writeStream.trigger(once=True).format("delta").start(
        path=output_path,
        checkpointLocation=checkpoint_location
    ).awaitTermination()

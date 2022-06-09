from .settings import Settings


def kafka_to_raw(spark, database, bootstrap_servers, topic):
    # TODO: make sure that only one instance of sparksession is running

    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_servers)
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        .load()
    )

    path: str = f'{database}/{topic}'
    output_path: str = f'{Settings.prefix_protocol}://{Settings.raw}/{path}'
    checkpoint_location: str = f'{Settings.prefix_protocol}://{Settings.spark_control}/_checkpoints/{path}'


    df.writeStream.trigger(once=True).format("delta").start(
        path=output_path,
        checkpointLocation=checkpoint_location
    ).awaitTermination()
from jibaro.settings import settings


def kafka_to_raw(spark, database, bootstrap_servers, topic):
    # TODO: make sure that only one instance of sparksession is running

    # TODO: Limit batch size in 1 million rows
    # spark.streaming.backpressure.enabled=true
    # [Optional] Tweak to balance data processing parallelism vs. task scheduling overhead (Default: 200ms)
    spark.conf.set("spark.streaming.blockInterval", "200")
    # Prevent data loss on driver recovery
    spark.conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    spark.conf.set("spark.streaming.backpressure.enabled", "true")
    # [Optional] Reduce min rate of PID-based backpressure implementation (Default: 100)
    spark.conf.set("spark.streaming.backpressure.pid.minRate", "10")
    # [Spark 1.x]: Workaround for missing initial rate (Default: not set)
    spark.conf.set("spark.streaming.receiver.maxRate", "100")
    # [Spark 1.x]: Corresponding max rate setting for Direct Kafka Streaming (Default: not set)
    spark.conf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    # [Spark 2.x]: Initial rate before backpressure kicks in (Default: not set)
    spark.conf.set("spark.streaming.backpressure.initialRate", "30")


    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_servers)
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        .option('maxOffsetsPerTrigger', 100)
        .load()
    )

    path: str = f'{database}/{topic}'
    output_path: str = f'{settings.prefix_protocol}://{settings.raw}/{path}'
    checkpoint_location: str = f'{settings.checkpoint_raw}/{path}'

    df.writeStream.trigger(once=True).format("delta").start(
        path=output_path,
        checkpointLocation=checkpoint_location
    ).awaitTermination()

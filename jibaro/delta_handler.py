from jibaro.settings import settings

def raw_to_staged(spark, database, table_name):

    path: str = f'{database}/{table_name}'
    source_path: str = f'{settings.prefix_protocol}://{settings.raw}/{path}'
    output_path: str = f'{settings.prefix_protocol}://{settings.staged}/{path}'

    df = (
        spark.readStream.format('delta').load(source_path)
    )

    checkpoint_location: str = f'{settings.checkpoint_paths}/{path}'

    df.writeStream.trigger(once=True).format("delta").start(
        path=output_path,
        checkpointLocation=checkpoint_location
    ).awaitTermination()

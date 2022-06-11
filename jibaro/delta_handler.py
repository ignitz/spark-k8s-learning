from jibaro.settings import settings
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient

def raw_to_staged(spark, database, table_name):
    sc=spark.sparkContext

    path: str = f'{database}/{table_name}'
    source_path: str = f'{settings.prefix_protocol}://{settings.raw}/{path}'
    output_path: str = f'{settings.prefix_protocol}://{settings.staged}/{path}'
    checkpoint_location: str = f'{settings.checkpoint_staged}/{path}'
    
    schemaRegistryUrl: str = settings.schemaRegistryUrl
    fromAvroOptions = {"mode":"FAILFAST"}
    schema_registry_conf = {
        'url': schemaRegistryUrl,
        # 'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())
    def getSchema(id):
        return str(schema_registry_client.get_schema(id).schema_str)

    df = (
        spark.readStream.format('delta').load(source_path)
    )


    # Option to process avro binary
    def process_confluent_schemaregistry(df_batch, batch_id):
        print(f"batch_id: {batch_id}")

        # drop_duplicates only works if key stay in the same partition
        # df_batch = df_batch.orderBy(fn.col('timestamp').desc()).drop_duplicates(['key'])
        df_change = (
            df_batch
            .withColumn('keySchemaId', binary_to_string(fn.expr("substring(key, 2, 4)")))
            .withColumn('key', fn.expr("substring(key, 6, length(value)-5)"))
            .withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
            .withColumn('value', fn.expr("substring(value, 6, length(value)-5)"))
        )
        distinctSchemaIdDF = (
            df_change
            .select(
                fn.col('keySchemaId').cast('integer'),
                fn.col('valueSchemaId').cast('integer')
            ).distinct().orderBy('keySchemaId', 'valueSchemaId')
        )

        for valueRow in distinctSchemaIdDF.collect():
            currentKeySchemaId = sc.broadcast(valueRow.keySchemaId)
            currentKeySchema = sc.broadcast(getSchema(currentKeySchemaId.value))

            currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
            currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))

            filterDF = df_change.filter(
                (fn.col('keySchemaId') == currentKeySchemaId.value)
                &
                (fn.col('valueSchemaId') == currentValueSchemaId.value)
            )

            (
                filterDF.select(
                    from_avro('key', currentKeySchema.value, fromAvroOptions).alias('key'),
                    from_avro('value', currentValueSchema.value, fromAvroOptions).alias('value'),
                    'topic',
                    'partition',
                    'offset',
                    'timestamp',
                    'timestampType',
                )
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(output_path)
            )
    ###############################################################
    (
        df
        .writeStream
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(process_confluent_schemaregistry)
        .start().awaitTermination()
    )
    print("writeStream Done")
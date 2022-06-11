from jibaro.settings import settings
from jibaro.utils import path_exists, delete_path
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from delta import DeltaTable


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
                    fn.col('keySchemaId').cast('integer'),
                    fn.col('valueSchemaId').cast('integer'),
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


def staged_to_curated(spark, database, table_name):
    sc=spark.sparkContext

    path: str = f'{database}/{table_name}'
    source_path: str = f'{settings.prefix_protocol}://{settings.staged}/{path}'
    output_path: str = f'{settings.prefix_protocol}://{settings.curated}/{path}'
    checkpoint_location: str = f'{settings.checkpoint_curated}/{path}'
    
    schemaRegistryUrl: str = settings.schemaRegistryUrl
    schema_registry_conf = {
        'url': schemaRegistryUrl,
        # 'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    def getSchema(id):
        return str(schema_registry_client.get_schema(id).schema_str)


    if not path_exists(spark, output_path):
        # delete checkpoint and process entire staged and create folder
        delete_path(spark, checkpoint_location)

    df = (
        spark.readStream.format('delta').load(source_path)
    )

    # Option to process avro binary
    def process_delta(df_batch, batch_id):
        print(f"batch_id: {batch_id}")

        # drop_duplicates only works if key stay in the same partition
        # df_batch = df_batch.orderBy(fn.col('timestamp').desc()).drop_duplicates(['key'])
        
        # Drop duplication with window partition
        df_batch = df_batch.withColumn(
            '_row_num',
            fn.row_number().over(
                Window.partitionBy('key').orderBy(fn.col('timestamp').desc())
            ),
        ).filter(fn.col('_row_num') == 1).drop('_row_num')
        
        distinctSchemaIdDF = (
            df_batch
            .select(
                'keySchemaId',
                'valueSchemaId',
            ).distinct().orderBy('keySchemaId', 'valueSchemaId')
        )

        for valueRow in distinctSchemaIdDF.collect():
            currentKeySchemaId = sc.broadcast(valueRow.keySchemaId)
            currentKeySchema = sc.broadcast(getSchema(currentKeySchemaId.value))

            currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
            currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))

            filterDF = df_batch.filter(
                (fn.col('keySchemaId') == currentKeySchemaId.value)
                &
                (fn.col('valueSchemaId') == currentValueSchemaId.value)
            )

            # TODO: process each column with SchemaRegistry
            # currentValueSchema

            if not path_exists(spark, output_path):
                dfUpdated = filterDF.filter("value.op != 'd'").select("value.after.*", "value.op")
                (
                    dfUpdated
                    .write
                    .format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .save(output_path)
                )
            else:
                if not DeltaTable.isDeltaTable(spark, output_path):
                    # Try to convert to Delta ?
                    raise NotImplemented
                dt = DeltaTable.forPath(spark, output_path)
                
                dfUpdated = filterDF.filter("value.op != 'd'").select("value.after.*", "value.op").union(
                    filterDF.filter("value.op = 'd'").select("value.before.*", "value.op")
                )

                (
                    dt.alias('table')
                        .merge(
                            dfUpdated.alias("update"),
                            ' AND '.join([
                                f'table.`{pk}` = update.`{pk}`'
                                for pk in filterDF.select('key.*').columns
                            ])
                        )
                        .whenMatchedUpdateAll(condition="update.op != 'd'")
                        .whenNotMatchedInsertAll(condition="update.op != 'd'")
                        .whenMatchedDelete(condition="update.op = 'd'")
                        .execute()
                )
    ###############################################################
    (
        df
        .writeStream
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(process_delta)
        .start().awaitTermination()
    )
    print("writeStream Done")
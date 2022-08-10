
from jibaro.datalake.delta_handler import compact_files
from jibaro.datalake.path import mount_checkpoint_path, mount_path, mount_history_path
from jibaro.settings import settings
from jibaro.utils import path_exists, delete_path
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from delta import DeltaTable

__all__ = ["kafka_to_raw", "raw_to_staged", "staged_to_curated"]


def kafka_to_raw(spark, output_layer, bootstrap_servers, topic):
    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_servers)
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        # Will work only with Spark 3.3 with
        .option('maxOffsetsPerTrigger', 100)
        .load()
    )

    # TODO: expected with less than two dots
    project_name, database, table_name = topic.split('.')

    df.writeStream.trigger(once=True).format("delta").start(
        layer=output_layer,
        project_name=project_name,
        database=database,
        table_name=table_name
    ).awaitTermination()


def avro_handler(spark, source_layer, target_layer, project_name, database, table_name, schema_registry):
    sc = spark.sparkContext

    schemaRegistryUrl: str = schema_registry['url']
    fromAvroOptions = {"mode": "FAILFAST"}
    schema_registry_conf = {
        'url': schemaRegistryUrl,
        # 'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    binary_to_string = fn.udf(lambda x: str(
        int.from_bytes(x, byteorder='big')), StringType())

    def getSchema(id):
        return str(schema_registry_client.get_schema(id).schema_str)

    df = (
        spark.readStream.format('delta').load(
            layer=source_layer,
            project_name=project_name,
            database=database,
            table_name=table_name
        )
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
            currentKeySchema = sc.broadcast(
                getSchema(currentKeySchemaId.value))

            currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
            currentValueSchema = sc.broadcast(
                getSchema(currentValueSchemaId.value))

            print(f"currentKeySchemaId: {currentKeySchemaId}")
            print(f"currentKeySchema: {currentKeySchema}")
            print(f"currentValueSchemaId: {currentValueSchemaId}")
            print(f"currentValueSchema: {currentValueSchema}")

            filterDF = df_change.filter(
                (fn.col('keySchemaId') == currentKeySchemaId.value)
                &
                (fn.col('valueSchemaId') == currentValueSchemaId.value)
            )

            (
                filterDF.select(
                    from_avro('key', currentKeySchema.value,
                              fromAvroOptions).alias('key'),
                    from_avro('value', currentValueSchema.value,
                              fromAvroOptions).alias('value'),
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
                .save(
                    mount_path(
                        layer=target_layer,
                        project_name=project_name,
                        database=database,
                        table_name=table_name
                    )
                )
            )
    ###############################################################
    (
        df
        .writeStream
        .trigger(once=True)
        .option("checkpointLocation", mount_checkpoint_path(target_layer, project_name, database, table_name))
        .foreachBatch(process_confluent_schemaregistry)
        .start().awaitTermination()
    )
    print("writeStream Done")


def raw_to_staged(spark, project_name, database, table_name, environment, content_type='avro'):
    schema_registry = settings.schemaRegistry[environment]

    if content_type == 'avro':
        avro_handler(spark=spark, source_layer='raw', target_layer='staged', project_name=project_name, database=database,
                     table_name=table_name, schema_registry=schema_registry)
    else:
        raise NotImplemented


def staged_to_curated(spark, project_name, database, table_name, environment):
    sc = spark.sparkContext
    schema_registry = settings.schemaRegistry[environment]

    schemaRegistryUrl: str = schema_registry['url']
    schema_registry_conf = {
        'url': schemaRegistryUrl,
        # 'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # def getSchema(id):
    #     return str(schema_registry_client.get_schema(id).schema_str)

    source_layer = 'staged'
    target_layer = 'curated'
    output_path = mount_path(
        layer=target_layer, project_name=project_name, database=database, table_name=table_name)
    checkpoint_location = mount_checkpoint_path(
        layer=target_layer, project_name=project_name, database=database, table_name=table_name)
    history_path = mount_history_path(
        layer=target_layer, project_name=project_name, database=database, table_name=table_name)

    if not path_exists(spark, output_path):
        # delete checkpoint and process entire staged and create folder
        delete_path(spark, checkpoint_location)

    df = (
        spark.readStream.format('delta').load(
            layer=source_layer, project_name=project_name, database=database, table_name=table_name
        )
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

        key_schema_column = 'keySchemaId' if 'keySchemaId' in df.columns else 'keySchema'
        value_schema_column = 'valueSchemaId' if 'valueSchemaId' in df.columns else 'valueSchema'

        df_batch.columns

        distinctSchemaIdDF = (
            df_batch
            .select(
                key_schema_column,
                value_schema_column,
            ).distinct().orderBy(key_schema_column, value_schema_column)
        )

        for valueRow in distinctSchemaIdDF.collect():
            currentKeySchemaId = sc.broadcast(
                valueRow.asDict()[key_schema_column])
            # currentKeySchema = sc.broadcast(getSchema(currentKeySchemaId.value))

            currentValueSchemaId = sc.broadcast(
                valueRow.asDict()[value_schema_column])
            # currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))

            filterDF = df_batch.filter(
                (fn.col(key_schema_column) == currentKeySchemaId.value)
                &
                (fn.col(value_schema_column) == currentValueSchemaId.value)
            )

            # TODO: process each column with SchemaRegistry
            # currentValueSchema

            if not path_exists(spark, output_path):
                dfUpdated = filterDF.filter("value.op != 'd'").select(
                    "value.after.*", "value.op")
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

                # Need to fill payload before with schema.
                dfUpdated = filterDF.filter("value.op != 'd'").select("value.after.*", "value.op").union(
                    filterDF.filter("value.op = 'd'").select(
                        "value.before.*", "value.op")
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
                # end else

            need_compact, numFiles, numOfPartitions = compact_files(
                spark=spark, target_path=output_path)

            # Generate metrics
            dt = DeltaTable.forPath(spark, output_path)

            if need_compact:
                max_version = (
                    dt.history(2).select(
                        fn.max('version').alias('max_version'))
                    .first().max_version
                )
                dt.history(2).withColumn(
                    "numFiles", fn.when(fn.col("version") ==
                                        max_version, numOfPartitions)
                    .otherwise(numFiles)
                ) \
                    .write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save(history_path)
            else:
                dt.history(1).withColumn(
                    "numFiles", fn.lit(numFiles)
                ) \
                    .write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save(history_path)

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

    # Generate manifest
    dt = DeltaTable.forPath(spark, output_path)
    dt.generate("symlink_format_manifest")

    # Do vacuum process every 25 versions
    version = (
        dt.history(2).select(fn.max('version').alias('max_version'))
        .first().max_version
    )
    if version % 25 == 0 and version > 0:
        dt.vacuum(retentionHours=768)

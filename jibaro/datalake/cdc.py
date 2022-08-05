from jibaro.datalake.delta_handler import compact_files
from jibaro.settings import settings
from jibaro.utils import path_exists, delete_path
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType, StructType
from pyspark.sql.window import Window
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from delta import DeltaTable
import json


def kafka_to_raw(spark, database, bootstrap_servers, topic):
    df = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_servers)
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        # Will work only with Spark 3.3 with
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


def avro_handler(spark, database, table_name, schema_registry):
    sc = spark.sparkContext

    path: str = f'{database}/{table_name}'
    source_path: str = f'{settings.prefix_protocol}://{settings.raw}/{path}'
    output_path: str = f'{settings.prefix_protocol}://{settings.staged}/{path}'
    checkpoint_location: str = f'{settings.checkpoint_staged}/{path}'

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


def json_handler(spark, database, table_name):
    """Trying to develop with:

    # "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    # "key.converter.schemas.enable": "true",
    # "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    # "value.converter.schemas.enable": "true"
    """

    path: str = f'{database}/{table_name}'
    source_path: str = f'{settings.prefix_protocol}://{settings.raw}/{path}'
    output_path: str = f'{settings.prefix_protocol}://{settings.staged}/{path}'
    checkpoint_location: str = f'{settings.checkpoint_staged}/{path}'
    schema_path: str = f'{output_path}/_schema'

    df = (
        spark.readStream.format('delta').load(source_path)
    )

    def process_debezium_json(df_batch, batch_id):
        # https://github.com/apache/spark/pull/22775
        df_batch = (
            df_batch.withColumn('key', fn.col('key').cast('string'))
            .withColumn('value', fn.col('value').cast('string'))
        )

        key_schema = None
        key_payload_schema = None
        value_schema = None
        value_payload_schema = None

        will_store_schema = False
        if not path_exists(spark, schema_path):
            df_key = spark.read.json(
                df_batch.select(
                    fn.col('key')
                ).rdd.map(lambda x: x.key)
            )
            key_schema = StructType(
                df_key.select('schema').schema
            ).json()
            key_payload_schema = StructType(
                df_key.select('payload').schema
            ).json()
            df_value = spark.read.json(
                df_batch.select(
                    fn.col('value')
                ).rdd.map(lambda x: x.value)
            )
            value_schema = StructType(
                df_value.select('schema').schema
            ).json()
            value_payload_schema = StructType(
                df_value.select('payload').schema
            ).json()
            # write to schema_path later
            will_store_schema = True
        else:
            # get schema
            schemas = spark.read.parquet(schema_path).first()
            key_payload_schema = schemas.key
            value_payload_schema = schemas.value

        # df_change = (
        #     df_batch.withColumn('keySchema', fn.from_json(fn.col('key').cast('string'), StructType.fromJson(json.loads(key_schema))))
        #     .withColumn('keySchema', fn.to_json(fn.col('keySchema.schema')))
        #     .withColumn('valueSchema', fn.from_json(fn.col('value').cast('string'), StructType.fromJson(json.loads(value_schema))))
        #     .withColumn('valueSchema', fn.to_json(fn.col('valueSchema.schema')))
        # )
        df_change = (
            df_batch.withColumn('keySchema', fn.lit(key_payload_schema))
            .withColumn('valueSchema', fn.lit(value_payload_schema))
        )

        distinctSchemaIdDF = (
            df_change
            .select('keySchema', 'valueSchema')
            .distinct().orderBy('keySchema', 'valueSchema')
        )

        for valueRow in distinctSchemaIdDF.collect():
            currentKeySchema = valueRow.keySchema
            currentValueSchema = valueRow.valueSchema

            print(f"currentKeySchema: {currentKeySchema}")
            print(f"currentValueSchema: {currentValueSchema}")

            filterDF = df_change.filter(
                (fn.col('keySchema') == currentKeySchema)
                &
                (fn.col('valueSchema') == currentValueSchema)
            )
            # TODO: Don´t work yet. Need to understanding debezium schema vs spark schema
            (
                filterDF.select(
                    fn.from_json('key', StructType.fromJson(
                        json.loads(currentKeySchema))).alias('key'),
                    fn.from_json('value', StructType.fromJson(
                        json.loads(currentValueSchema))).alias('value'),
                    'topic',
                    'partition',
                    'offset',
                    'timestamp',
                    'timestampType',
                    'keySchema',
                    'valueSchema',
                ).withColumn('key', fn.col('key.payload'))
                .withColumn('value', fn.col('value.payload'))
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(output_path)
            )

            if will_store_schema:
                spark.createDataFrame([{
                    'key': key_payload_schema,
                    'value': value_payload_schema
                }]).write.mode('overwrite').parquet(schema_path)
    ###############################################################
    (
        df
        .writeStream
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(process_debezium_json)
        .start().awaitTermination()
    )


def raw_to_staged(spark, database, table_name, environment, content_type='avro'):
    schema_registry = settings.schemaRegistry[environment]

    if content_type == 'avro':
        avro_handler(spark=spark, database=database,
                     table_name=table_name, schema_registry=schema_registry)
    elif content_type == 'json':
        json_handler(spark=spark, database=database, table_name=table_name)
    else:
        raise NotImplemented


def staged_to_curated(spark, database, table_name, environment):
    sc = spark.sparkContext
    schema_registry = settings.schemaRegistry[environment]

    path: str = f'{database}/{table_name}'
    source_path: str = f'{settings.prefix_protocol}://{settings.staged}/{path}'
    output_path: str = f'{settings.prefix_protocol}://{settings.curated}/{path}'
    history_path: str = f'{settings.prefix_protocol}://{settings.curated}/_history/{path}'
    checkpoint_location: str = f'{settings.checkpoint_curated}/{path}'

    schemaRegistryUrl: str = schema_registry['url']
    schema_registry_conf = {
        'url': schemaRegistryUrl,
        # 'basic.auth.user.info': '{}:{}'.format(confluentRegistryApiKey, confluentRegistrySecret)
    }
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    # def getSchema(id):
    #     return str(schema_registry_client.get_schema(id).schema_str)

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

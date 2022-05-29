# More info in https://www.confluent.io/blog/consume-avro-data-from-kafka-topics-and-secured-schema-registry-with-databricks-confluent-cloud-on-azure/#step-4
# !pip install 'confluent-kafka[avro,json,protobuf]>=1.4.2'

import os
os.system("pip install 'confluent-kafka[avro,json,protobuf]>=1.4.2'")

import sys
from pyspark.sql import SparkSession
from confluent_kafka.schema_registry import SchemaRegistryClient
import ssl
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType
from pyspark.sql.avro.functions import from_avro

spark = SparkSession.builder.appName("Spark Streaming Delta").getOrCreate()

topic = 'dbserver1.inventory.products'
schemaRegistryUrl = "http://schema-registry:8081"

schema_registry_conf = {
    'url': schemaRegistryUrl
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())

def parseAvroDataWithSchemaId(df, ephoch_id):
    cachedDf = df.cache()
    
    fromAvroOptions = {"mode":"FAILFAST"}
    
    def getSchema(id):
        return str(schema_registry_client.get_schema(id).schema_str)

    distinctValueSchemaIdDF = cachedDf.select(fn.col('valueSchemaId').cast('integer')).distinct()

    for valueRow in distinctValueSchemaIdDF.collect():
        currentValueSchemaId = sc.broadcast(valueRow.valueSchemaId)
        currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))
    
        filterValueDF = cachedDf.filter(fn.col('valueSchemaId') == currentValueSchemaId.value)
        
        filterValueDF \
            .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', from_avro('fixedValue', currentValueSchema.value, fromAvroOptions).alias('parsedValue')) \
            .write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(f's3a://datalake/data/{topic}')
import math
from delta import DeltaTable
from jibaro.settings import settings

def compact_files(spark, target_path):
    if DeltaTable.isDeltaTable(spark, target_path):
        # DOESN'T NOT WORK in old Delta Lake versions than 1.0.0
        # details = spark.sql(f"describe detail 's3a://datalake-curated/kafka/airflow.public.log'").first()
        details = spark.sql(f"describe detail '{target_path}'").first()

        # https://docs.delta.io/latest/best-practices.html#compact-files
        need_compact = (
            True
            if details.numFiles > settings.max_num_files_allowed
            else False
        )

        numOfPartitions = None

        if need_compact: 
            # Compact files
            numOfPartitions = math.ceil(details.sizeInBytes / (1024 * 1024))
            (
                spark.read.format('delta').load(target_path)
                .repartition(numOfPartitions)
                .write
                .option("dataChange", "false")
                .format("delta")
                .mode("overwrite")
                .save(target_path)
            )
        return need_compact, details.numFiles, numOfPartitions

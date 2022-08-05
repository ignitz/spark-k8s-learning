from jibaro.spark.session import JibaroSession

spark = JibaroSession.builder.appName("JibaroSession").getOrCreate()

# This will read
df = spark.read.load(layer="curated", project_name="project",
                     database="kafka", table_name="dbserver1.inventory.products",
                     format="delta")

df.show()

df.write.mode("overwrite").parquet(
    layer="curated", project_name="project", database="kafka", table_name="path_test")

def path_exists(spark, path):
    # spark is a SparkSession
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
    ###############################
    # This method support wildcard
    # hpath = sc._jvm.org.apache.hadoop.fs.Path(path)
    # fs = hpath.getFileSystem(sc._jsc.hadoopConfiguration())
    # return len(fs.globStatus(hpath)) > 0

def delete_path(spark, path):
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create(path),
        sc._jsc.hadoopConfiguration(),
    )
    if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path)):
        return fs.delete(sc._jvm.org.apache.hadoop.fs.Path(str(path)))
    return False

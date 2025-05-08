from pyspark.sql import SparkSession
import random

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("Not Disable AQE") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:/tmp/spark-events") \
    .config("spark.driver.cores", "1") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "1") \
    .config("spark.cores.max", "1") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.dynamicAllocation.initialExecutors", "1") \
    .config("spark.dynamicAllocation.minExecutors", "1") \
    .config("spark.dynamicAllocation.maxExecutors", "1") \
    .getOrCreate()

# Chemin vers le bucket S3
skewed_path = "s3a://default-bucket/skewed_data/"


skewed_df = spark.read.option("header", "true").option("inferSchema", "false").csv(skewed_path)
skewed_df.explain(mode="extended")
count = skewed_df.count()
print(f"Nombre de lignes dans le DataFrame : {count}")



# Écriture des datasets sur S3
#skewed_df.write.csv(skewed_path, mode="overwrite")

#print(f"Skewed Data écrit sur {skewed_path}")
# Arrêt de la session Spark
spark.stop()

from pyspark.sql import SparkSession
from time import time

# 📌 Création de la session Spark
spark = SparkSession.builder \
    .appName("HeavyComputeOnExecutors") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "3") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:/tmp/spark-events") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

#  Création d'un RDD volumineux simulant un gros dataset
data = [(i, i % 100) for i in range(10_000_000)]
rdd = spark.sparkContext.parallelize(data)

#  Temps de traitement côté Driver (très court)
start_driver_time = time()
rdd.count()  # Juste pour forcer le chargement
end_driver_time = time()
driver_time = end_driver_time - start_driver_time

#  Travail intensif sur les Exécutors
start_executor_time = time()
result = rdd.mapPartitions(lambda iter: [(x[0], x[1] ** 2) for x in iter]) \
            .reduceByKey(lambda a, b: a + b)
end_executor_time = time()
executor_time = end_executor_time - start_executor_time

# Résumé des temps
print(f"Temps côté Driver : {driver_time:.2f} sec")
print(f"Temps côté Exécutors : {executor_time:.2f} sec")

#  Vérification d'un petit aperçu des résultats
print(result.take(5))

# 🔄 Fermeture de Spark
spark.stop()

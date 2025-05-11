from pyspark.sql import SparkSession


# ⚡ Initialiser Spark
spark = SparkSession.builder \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:/tmp/spark-events") \
    .getOrCreate()

    #.appName("Memory Intensive Job - 512m") \
    #.config("spark.executor.memory", "512m") \
    #.config("spark.executor.memory", "4g") \
    #.config("spark.driver.memory", "4g")\

# 🔥 Générer un grand DataFrame artificiel (10M lignes)
data = [(i % 10000, i * 1.5) for i in range(10_000_000)]
df = spark.createDataFrame(data, ["id", "value"])

# 🔄 Opération lourde sur la mémoire : `groupBy()` + `agg()`
df_grouped = df.groupBy("id").agg({"value": "sum"})

# 🏋️‍♂️ Forcer la mise en mémoire (cache massif)
df_grouped.cache()

# ⏳ Déclencher l’exécution complète
df_grouped.show()

# 🔍 Afficher l'utilisation mémoire Spark
executor_memory = spark._jsc.sc().getExecutorMemoryStatus()
print("💾 Mémoire des exécutors :", executor_memory)

# 🚀 Arrêter Spark
spark.stop()

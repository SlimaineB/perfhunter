from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("SimpleCode Job")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:/tmp/spark-events")
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "1") \
        .config("spark.cores.max", "1") \
        .getOrCreate()
)

# Lire un fichier CSV
#df = spark.read.csv("file:///opt/spark-apps/jobs/people.csv", header=True, inferSchema=True)
#df.createOrReplaceTempView("table_temp")

df = spark.createDataFrame(
    [(1, "Alice", 25, "Paris"), (2, "Bob", 32, "Lyon")],
    ["id", "name", "age", "city"]
)

# Opération 1 : Filtrer les données
filtered_df = df.filter(df["age"] > 30)

# Opération 2 : GroupBy et agrégation
result_df = filtered_df.groupBy("city").count()

result_df.count()

# Afficher le résultat
#result_df.show()

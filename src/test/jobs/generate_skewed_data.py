from pyspark.sql import SparkSession
import random

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("SkewedGroupByJob")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:/tmp/spark-events")
        .config("spark.executor.instances", "3")
        .getOrCreate()
    )

    # Génère un skew extrême : 99.9% des lignes sur "skewed"
    data = [("skewed", random.randint(1, 100)) for _ in range(999_000)]
    data += [("normal_" + str(i), random.randint(1, 100)) for i in range(1000)]
    df = spark.createDataFrame(data, ["key", "value"])

    # Shuffle massif sur la clé 'key' (data skew sur 'skewed')
    result = df.groupBy("key").sum("value").repartition(3, df["key"])
    result.show(20, truncate=False)

    # Affiche la taille de chaque partition après le groupBy
    def partition_sizes(iterator):
        yield sum(1 for _ in iterator)
    sizes = result.rdd.mapPartitions(partition_sizes).collect()
    print("Partition sizes after groupBy:", sizes)

    spark.stop()
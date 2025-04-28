from pyspark.sql import SparkSession
import random

def calculate_pi(partitions):
    def inside(_):
        x, y = random.random(), random.random()
        return x * x + y * y < 1

    n = 100000 * partitions
    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).filter(inside).count()
    return 4.0 * count / n

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("CalculatePi")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:/tmp/spark-events")
        .getOrCreate()
    )
    partitions = 10
    pi = calculate_pi(partitions)
    print(f"Pi is roughly {pi}")
    spark.stop()
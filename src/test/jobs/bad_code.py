from pyspark.sql import SparkSession
import random

def generate_data(spark, num_rows):
    # Génère une DataFrame avec des données aléatoires non optimisées
    data = [(random.randint(1, 100), random.random()) for _ in range(num_rows)]
    return spark.createDataFrame(data, ["id", "value"])

def process_data(df):
    # Effectue des transformations non optimisées
    df = df.filter(df["id"] > 50)  # Filtre les id > 50
    df = df.withColumnRenamed("value", "random_value")  # Renomme une colonne
    df = df.sort("random_value")  # Trie par valeur aléatoire
    return df

def main():
    spark = SparkSession.builder \
        .appName("Non-Optimized PySpark Application") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "file:/tmp/spark-events") \
        .getOrCreate()

    # Génère une grande quantité de données non optimisées
    df = generate_data(spark, 1000000)

    # Traite les données de manière inefficace
    processed_df = process_data(df)

    # Affiche les 10 premières lignes
    processed_df.show(10)

    spark.stop()

if __name__ == "__main__":
    main()
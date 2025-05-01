from pyspark.sql import SparkSession
import random

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("Data Skew Test") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:/tmp/spark-events") \
    .getOrCreate()

# Chemin vers le bucket S3
skewed_path = "s3a://default-bucket/skewed_data/"
balanced_path = "s3a://default-bucket/balanced_data/"

# Nombre de lignes et colonnes
#num_rows = 10_000  # Ajustez pour tester différentes tailles
num_rows = 1000  # Ajustez pour tester différentes tailles
num_columns = 5

# Génération des données déséquilibrées (Skewed Data)
def generate_skewed_data(num_rows, num_columns):
    data = []
    for _ in range(num_rows):
        # Une clé unique apparaît beaucoup plus souvent pour simuler le skew
        key = random.choice([1] * 90 + [2] * 5 + [3] * 5)  # Clé très déséquilibrée
        row = [key] + [random.random() for _ in range(num_columns - 1)]
        data.append(row)
    return data

# Génération des données équilibrées (Balanced Data)
def generate_balanced_data(num_rows, num_columns):
    data = []
    for _ in range(num_rows):
        # Les clés sont uniformément réparties
        key = random.choice([1, 2, 3])
        row = [key] + [random.random() for _ in range(num_columns - 1)]
        data.append(row)
    return data

# Conversion en DataFrame pour Skewed Data
skewed_data = spark.sparkContext.parallelize(generate_skewed_data(num_rows, num_columns))
skewed_df = skewed_data.toDF(["key"] + [f"col_{i}" for i in range(1, num_columns)])

# Conversion en DataFrame pour Balanced Data
balanced_data = spark.sparkContext.parallelize(generate_balanced_data(num_rows, num_columns))
balanced_df = balanced_data.toDF(["key"] + [f"col_{i}" for i in range(1, num_columns)])

# Écriture des datasets sur S3
skewed_df.write.csv(skewed_path, mode="overwrite")
balanced_df.write.csv(balanced_path, mode="overwrite")

print(f"Skewed Data écrit sur {skewed_path}")
print(f"Balanced Data écrit sur {balanced_path}")

# Arrêt de la session Spark
spark.stop()

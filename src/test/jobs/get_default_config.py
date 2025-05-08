from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
default_configs = spark.sparkContext.getConf().getAll()

for key, value in default_configs:
    print(f"{key}: {value}")

def get_config_value(spark, key):
    try:
        return spark.conf.get(key)
    except:
        return "Non défini"

spark_configs = ["spark.sql.shuffle.partitions", "spark.executor.memory", "spark.driver.memory"]

for config in spark_configs:
    print(f"{config}: {get_config_value(spark, config)}")

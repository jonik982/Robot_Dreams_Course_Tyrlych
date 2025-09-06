from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("RawToBronzeCustomers") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Знаходимо останню підпапку
base_path = "/opt/airflow/dags/python_scripts/data/customers/"
subdirs = sorted([d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))])
latest_subdir = subdirs[-1]  # припускаємо, що формат YYYY-MM-DD і сортування працює

raw_path = os.path.join(base_path, latest_subdir)
bronze_path = "/opt/airflow/dags/python_scripts/data/bronze/customers/"

# Read latest dump only
df = spark.read.option("header", True).csv(raw_path)

# Write to bronze
df.write.mode("overwrite").parquet(bronze_path)

spark.stop()
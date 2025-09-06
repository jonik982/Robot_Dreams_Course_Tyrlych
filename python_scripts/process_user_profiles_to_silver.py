from pyspark.sql import SparkSession

# Ініціалізація Spark
spark = SparkSession.builder \
    .appName("ProcessUserProfiles") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Paths
raw_path = "/opt/airflow/dags/python_scripts/data/user_profiles/"
silver_path = "/opt/airflow/dags/python_scripts/data/silver/user_profiles/"

# Read JSONLine
df = spark.read.json(raw_path)

# Optional: schema validation / null check
df_clean = df.select("email", "full_name", "state", "birth_date", "phone_number") \
    .dropna(subset=["email", "full_name", "state", "birth_date", "phone_number"])

# Write to silver
df_clean.write.mode("overwrite").parquet(silver_path)

spark.stop()
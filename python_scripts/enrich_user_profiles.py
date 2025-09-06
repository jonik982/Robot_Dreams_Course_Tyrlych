from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split

# Ініціалізація Spark
spark = SparkSession.builder \
    .appName("EnrichUserProfiles") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Шляхи
customers_silver_path = "/opt/airflow/dags/python_scripts/data/silver/customers/"
profiles_silver_path = "/opt/airflow/dags/python_scripts/data/silver/user_profiles/"
gold_output_path = "/opt/airflow/dags/python_scripts/data/gold/user_profiles_enriched/"

# Читання
customers_df = spark.read.parquet(customers_silver_path)
profiles_df = spark.read.parquet(profiles_silver_path)

# Спліт full_name → first_name + last_name, якщо потрібно
split_name = split(col("full_name"), " ")
profiles_df = profiles_df.withColumn("profile_first_name", split_name.getItem(0)) \
                         .withColumn("profile_last_name", split_name.getItem(1))

# Join за email
enriched_df = customers_df.alias("cust").join(
    profiles_df.alias("prof"),
    on=col("cust.email") == col("prof.email"),
    how="left"
)

# Збагачення полів
result_df = enriched_df.select(
    col("cust.client_id"),
    when(col("cust.first_name").isNull(), col("profile_first_name")).otherwise(col("cust.first_name")).alias("first_name"),
    when(col("cust.last_name").isNull(), col("profile_last_name")).otherwise(col("cust.last_name")).alias("last_name"),
    col("cust.email"),
    col("cust.registration_date"),
    when(col("cust.state").isNull(), col("prof.state")).otherwise(col("cust.state")).alias("state"),
    col("prof.birth_date"),
    col("prof.phone_number")
)

# Запис в gold
result_df.write.mode("overwrite").parquet(gold_output_path)

spark.stop()
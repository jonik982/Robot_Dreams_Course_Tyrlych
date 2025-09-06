from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, regexp_replace
from pyspark.sql.types import IntegerType

# Створення SparkSession з legacy parser для дат
spark = SparkSession.builder \
    .appName("CustomersBronzeToSilver") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Шляхи
bronze_path = "/opt/airflow/dags/python_scripts/data/bronze/customers/"
silver_path = "/opt/airflow/dags/python_scripts/data/silver/customers/"

# Читання bronze
df = spark.read.parquet(bronze_path)

# Фікс дати, наприклад '2022-08-3' => '2022-08-03'
df_fixed = df.withColumn(
    "RegistrationDate",
    regexp_replace("RegistrationDate", r"(\d{4}-\d{2}-)(\d{1})(?!\d)", r"$10$2")
)

# Очистка і мапінг колонок
df_clean = df_fixed \
    .withColumn("client_id", col("Id").cast(IntegerType())) \
    .withColumn("first_name", trim(col("FirstName"))) \
    .withColumn("last_name", trim(col("LastName"))) \
    .withColumn("email", trim(col("Email"))) \
    .withColumn("registration_date", to_date(col("RegistrationDate"), "yyyy-MM-dd")) \
    .withColumn("state", trim(col("State"))) \
    .select("client_id", "first_name", "last_name", "email", "registration_date", "state") \
    .dropna(subset=["client_id", "registration_date", "state"])  # критичні поля

# Запис у silver
df_clean.write.mode("overwrite").parquet(silver_path)

spark.stop()
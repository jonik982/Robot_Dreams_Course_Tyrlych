from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Ініціалізація SparkSession
spark = SparkSession.builder \
    .appName("LocalSalesETL") \
    .master("local[*]") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Шлях до всіх CSV-файлів у підпапках (рекурсивно)
raw_path = "/opt/airflow/dags/python_scripts/data/sales/"           # в цій папці лежать підпапки за датами
bronze_path = "/opt/airflow/dags/python_scripts/data/bronze/sales/"     # вихідна parquet папка

# Читання всіх CSV рекурсивно
df = spark.read.option("header", True) \
    .option("recursiveFileLookup", True) \
    .csv(raw_path)

# Приведення всіх колонок до string
df_clean = df.select(
    col("CustomerId").cast("string"),
    col("PurchaseDate").cast("string"),
    col("Product").cast("string"),
    col("Price").cast("string")
)

# Запис у parquet
df_clean.write.mode("overwrite").parquet(bronze_path)

spark.stop()

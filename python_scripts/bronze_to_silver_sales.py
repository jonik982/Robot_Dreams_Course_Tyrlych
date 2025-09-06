from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, regexp_replace
from pyspark.sql.types import IntegerType, DoubleType

# Spark session з legacy time parser
spark = SparkSession.builder \
    .appName("BronzeToSilverSales") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Paths
bronze_path = "/opt/airflow/dags/python_scripts/data/bronze/sales/"
silver_path = "/opt/airflow/dags/python_scripts/data/silver/sales/"

# Read bronze data
df = spark.read.parquet(bronze_path)

# Виправлення формату дати: додаємо 0, якщо день - однозначне число
df_fixed = df.withColumn(
    "PurchaseDate",
    regexp_replace("PurchaseDate", r"(\d{4}-\d{2}-)(\d{1})(?!\d)", r"$10$2")
)

# Очищення та приведення типів
df_cleaned = df_fixed \
    .withColumn("client_id", col("CustomerId").cast(IntegerType())) \
    .withColumn("purchase_date", to_date(col("PurchaseDate"), "yyyy-MM-dd")) \
    .withColumn("product_name", trim(col("Product"))) \
    .withColumn("price", col("Price").cast(DoubleType())) \
    .dropna(subset=["client_id", "purchase_date", "product_name", "price"]) \
    .filter(col("price") > 0) \
    .dropDuplicates(["client_id", "purchase_date", "product_name", "price"]) \
    .select("client_id", "purchase_date", "product_name", "price")

# Write to silver with partitioning
df_cleaned.write.mode("overwrite") \
    .partitionBy("purchase_date") \
    .parquet(silver_path)

spark.stop()
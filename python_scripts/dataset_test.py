from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, to_date, year, month, dayofmonth, current_date, datediff

spark = SparkSession.builder \
    .appName("AnalyticalQuery") \
    .master("local[*]") \
    .getOrCreate()

# Load data
sales_df = spark.read.parquet("C:/Users/Asus/PycharmProjects/lec07/airflow/dags/python_scripts/data/silver/sales/")
profiles_df = spark.read.parquet("C:/Users/Asus/PycharmProjects/lec07/airflow/dags/python_scripts/data/gold/user_profiles_enriched/")

# Додати вік користувача
profiles_df = profiles_df.withColumn("age", (datediff(current_date(), col("birth_date")) / 365.25).cast("int"))

# Join за client_id
df = sales_df.join(profiles_df, on="client_id", how="inner")

# Фільтрація: телевізори, дата, вік
filtered_df = df \
    .filter((lower(col("product_name")).like("%tv%")) |
            (lower(col("product_name")).like("%television%"))) \
    .filter((month("purchase_date") == 9) & (dayofmonth("purchase_date") <= 10)) \
    .filter((col("age") >= 20) & (col("age") <= 30))

# Групування по штатах і підрахунок
result = filtered_df.groupBy("state").count().orderBy(col("count").desc())

result.show(1)
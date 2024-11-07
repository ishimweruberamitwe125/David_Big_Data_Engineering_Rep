from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, monotonically_increasing_id

# Initialize Spark session and enable Hive support
spark = SparkSession.builder \
    .appName("WalmartStarSchema") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .config("hive.metastore.uris", "thrift://ip-172-31-1-36.eu-west-2.compute.internal:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Set the database to sept
spark.sql("USE sept")

# Helper function to create external Hive tables in sept database
def create_external_table(query, table_name, path):
    print(f"Creating table {table_name} at {path} in 'sept' database...")
    spark.sql(f"DROP TABLE IF EXISTS sept.{table_name}")
    spark.sql(query)
    print(f"Table {table_name} created at {path} in 'sept' database.")

# Define the paths for each table
base_path = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal/tmp/david/data_modelling"

# Load data from existing external Hive tables in the `sept` database
store_df = spark.sql("SELECT * FROM sept.store")
features_df = spark.sql("SELECT * FROM sept.features")
past_sales_df = spark.sql("SELECT * FROM sept.past_sales")

# 1. Create Store Dimension
store_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS sept.dim_store (
    store_id INT,
    store_type STRING,
    store_size INT,
    last_modified TIMESTAMP
)
STORED AS ORC
LOCATION '{base_path}/dim_store'
"""
create_external_table(store_table_query, "dim_store", f"{base_path}/dim_store")

# Insert data into dim_store
print("Loading data into dim_store...")
store_df.select("store", "type", "size", "last_modified") \
    .withColumnRenamed("store", "store_id") \
    .withColumnRenamed("type", "store_type") \
    .withColumnRenamed("size", "store_size") \
    .write.mode("overwrite").insertInto("sept.dim_store")
print("Data loaded into dim_store.")
# Display first 5 records from dim_store
spark.sql("SELECT * FROM sept.dim_store").show(5)

# 2. Create Date Dimension
date_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS sept.dim_date (
    date_id DATE,
    year INT,
    month INT,
    day INT
)
STORED AS ORC
LOCATION '{base_path}/dim_date'
"""
create_external_table(date_table_query, "dim_date", f"{base_path}/dim_date")

# Extract year, month, day and insert into dim_date
print("Loading data into dim_date...")
date_df = past_sales_df.select("date").distinct() \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date"))

date_df.select("date", "year", "month", "day").withColumnRenamed("date", "date_id") \
    .write.mode("overwrite").insertInto("sept.dim_date")
print("Data loaded into dim_date.")
# Display first 5 records from dim_date
spark.sql("SELECT * FROM sept.dim_date").show(5)

# 3. Create Department Dimension
department_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS sept.dim_department (
    dept_id INT,
    last_modified TIMESTAMP
)
STORED AS ORC
LOCATION '{base_path}/dim_department'
"""
create_external_table(department_table_query, "dim_department", f"{base_path}/dim_department")

# Insert data into dim_department
print("Loading data into dim_department...")
past_sales_df.select("dept", "last_modified").distinct() \
    .withColumnRenamed("dept", "dept_id") \
    .write.mode("overwrite").insertInto("sept.dim_department")
print("Data loaded into dim_department.")
# Display first 5 records from dim_department
spark.sql("SELECT * FROM sept.dim_department").show(5)

# 4. Create Holiday Dimension
holiday_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS sept.dim_holiday (
    holiday_id BOOLEAN,
    last_modified TIMESTAMP
)
STORED AS ORC
LOCATION '{base_path}/dim_holiday'
"""
create_external_table(holiday_table_query, "dim_holiday", f"{base_path}/dim_holiday")

# Insert data into dim_holiday
print("Loading data into dim_holiday...")
past_sales_df.select("isholiday", "last_modified").distinct() \
    .withColumnRenamed("isholiday", "holiday_id") \
    .write.mode("overwrite").insertInto("sept.dim_holiday")
print("Data loaded into dim_holiday.")
# Display first 5 records from dim_holiday
spark.sql("SELECT * FROM sept.dim_holiday").show(5)

# 5. Create Features Dimension and add feature_id
features_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS sept.dim_features (
    feature_id BIGINT,
    temperature FLOAT,
    fuel_price FLOAT,
    markdown1 FLOAT,
    markdown2 FLOAT,
    markdown3 FLOAT,
    markdown4 FLOAT,
    markdown5 FLOAT,
    cpi FLOAT,
    unemployment FLOAT,
    last_modified TIMESTAMP
)
STORED AS ORC
LOCATION '{base_path}/dim_features'
"""
create_external_table(features_table_query, "dim_features", f"{base_path}/dim_features")

# Add feature_id to features_df
print("Loading data into dim_features...")
features_df_with_id = features_df.withColumn("feature_id", monotonically_increasing_id())

features_df_with_id.select("feature_id", "temperature", "fuel_price", "markdown1", "markdown2",
                           "markdown3", "markdown4", "markdown5", "cpi", "unemployment", "last_modified") \
    .write.mode("overwrite").insertInto("sept.dim_features")
print("Data loaded into dim_features.")
# Display first 5 records from dim_features
spark.sql("SELECT * FROM sept.dim_features").show(5)

# 6. Create Fact Sales Table
fact_sales_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS sept.fact_sales (
    store_id INT,
    dept_id INT,
    date_id DATE,
    feature_id BIGINT,
    weekly_sales FLOAT,
    holiday_id BOOLEAN,
    last_modified TIMESTAMP
)
STORED AS ORC
LOCATION '{base_path}/fact_sales'
"""
create_external_table(fact_sales_table_query, "fact_sales", f"{base_path}/fact_sales")

# Insert data into fact_sales
print("Loading data into fact_sales...")
fact_sales_df = past_sales_df.alias("ps").join(
    features_df_with_id.alias("f"), (past_sales_df.store == features_df_with_id.store) & (past_sales_df.date == features_df_with_id.date)
).select(
    "ps.store", "ps.dept", "ps.date", "f.feature_id", "ps.weekly_sales", "ps.isholiday", "ps.last_modified"
).withColumnRenamed("store", "store_id") \
 .withColumnRenamed("dept", "dept_id") \
 .withColumnRenamed("date", "date_id") \
 .withColumnRenamed("isholiday", "holiday_id")

fact_sales_df.write.mode("overwrite").insertInto("sept.fact_sales")
print("Data loaded into fact_sales.")
# Display first 5 records from fact_sales
spark.sql("SELECT * FROM sept.fact_sales").show(5)

# Stop the Spark session
spark.stop()


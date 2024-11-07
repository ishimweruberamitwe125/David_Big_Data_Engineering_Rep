from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sql_sum, year, month, when
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import gc

# Initialize Spark session and enable Hive support
spark = SparkSession.builder \
    .appName("WalmartSalesForecasting") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .config("hive.metastore.uris", "thrift://ip-172-31-1-36.eu-west-2.compute.internal:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from Hive tables
fact_sales_df = spark.sql("SELECT * FROM sept.fact_sales")
dim_store_df = spark.sql("SELECT * FROM sept.dim_store")
dim_features_df = spark.sql("SELECT * FROM sept.dim_features")
dim_holiday_df = spark.sql("SELECT * FROM sept.dim_holiday")

# Convert PySpark DataFrame to Pandas for plotting
def to_pandas(spark_df):
    return spark_df.toPandas()

# Helper function to format values in millions
def format_millions(value, pos):
    return f'{value*1e-6:.1f}M'

# 1. Total Sales Over Time (Time Series Analysis)
sales_over_time = fact_sales_df.groupBy("date_id").agg(sql_sum("weekly_sales").alias("total_sales")).orderBy("date_id")
sales_over_time_pd = to_pandas(sales_over_time)

plt.figure(figsize=(10, 6))
plt.plot(pd.to_datetime(sales_over_time_pd['date_id']), sales_over_time_pd['total_sales'], color='blue', marker='o')
plt.title('Total Sales Over Time')
plt.xlabel('Date')
plt.ylabel('Total Sales (Millions)')
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_millions))  # Format in millions
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/home/ec2-user/image/total_sales_over_time.png')
plt.close()
gc.collect()

# 2. Sales by Month and Year (Time Series with Seasonality)
sales_by_month_year = fact_sales_df.groupBy(year("date_id").alias("year"), month("date_id").alias("month")).agg(sql_sum("weekly_sales").alias("total_sales")).orderBy("year", "month")
sales_by_month_year_pd = to_pandas(sales_by_month_year)

plt.figure(figsize=(10, 6))
sns.lineplot(data=sales_by_month_year_pd, x="month", y="total_sales", hue="year", palette="viridis", marker="o")
plt.title('Sales by Month and Year')
plt.xlabel('Month')
plt.ylabel('Total Sales (Millions)')
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_millions))  # Format in millions
plt.tight_layout()
plt.savefig('/home/ec2-user/image/sales_by_month_year.png')
plt.close()
gc.collect()

# 3. Holiday Impact on Sales (Histogram)
holiday_sales = fact_sales_df.join(dim_holiday_df, "holiday_id", "inner").groupBy("holiday_id").agg(sql_sum("weekly_sales").alias("total_sales"))
holiday_sales_pd = to_pandas(holiday_sales)

plt.figure(figsize=(8, 5))
sns.barplot(x=['Non-Holiday', 'Holiday'], y=holiday_sales_pd['total_sales'], palette="coolwarm")
plt.title('Holiday vs Non-Holiday Sales')
plt.ylabel('Total Sales (Millions)')
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_millions))  # Format in millions
plt.tight_layout()
plt.savefig('/home/ec2-user/image/holiday_vs_nonholiday_sales.png')
plt.close()
gc.collect()

# 4. Scatter plot: Fuel Price vs. Total Sales
fuel_sales_df = fact_sales_df.join(dim_features_df, "feature_id", "inner")
fuel_sales_pd = to_pandas(fuel_sales_df)

plt.figure(figsize=(10, 6))
sns.scatterplot(x=fuel_sales_pd['fuel_price'], y=fuel_sales_pd['weekly_sales'])
plt.title('Fuel Price vs. Total Sales')
plt.xlabel('Fuel Price')
plt.ylabel('Total Sales (Millions)')
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_millions))  # Format in millions
plt.tight_layout()
plt.savefig('/home/ec2-user/image/fuel_price_vs_total_sales.png')
plt.close()
gc.collect()

# 5. Scatter plot: CPI vs. Total Sales
plt.figure(figsize=(10, 6))
sns.scatterplot(x=fuel_sales_pd['cpi'], y=fuel_sales_pd['weekly_sales'])
plt.title('CPI vs. Total Sales')
plt.xlabel('CPI')
plt.ylabel('Total Sales (Millions)')
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_millions))  # Format in millions
plt.tight_layout()
plt.savefig('/home/ec2-user/image/cpi_vs_total_sales.png')
plt.close()
gc.collect()

# 6. Scatter plot: Unemployment Rate vs. Total Sales
plt.figure(figsize=(10, 6))
sns.scatterplot(x=fuel_sales_pd['unemployment'], y=fuel_sales_pd['weekly_sales'])
plt.title('Unemployment Rate vs. Total Sales')
plt.xlabel('Unemployment Rate')
plt.ylabel('Total Sales (Millions)')
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(format_millions))  # Format in millions
plt.tight_layout()
plt.savefig('/home/ec2-user/image/unemployment_rate_vs_total_sales.png')
plt.close()
gc.collect()

# Stop Spark session
spark.stop()


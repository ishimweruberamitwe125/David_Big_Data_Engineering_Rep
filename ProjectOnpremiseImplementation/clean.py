from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col, udf, isnan, when, first, stddev, to_date, count
from pyspark.sql.types import StringType, FloatType, DateType
from pyspark.sql.window import Window

# Initialize Spark session and configure legacy time parsing
spark = SparkSession.builder \
    .appName("DataCleaning") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Function to load data from HDFS
def load_data(hdfs_path):
    return spark.read.option("header", "true").csv(hdfs_path)

# Function to clean feature data (drop nulls)
def clean_feature_data(df):
    # Initial counts
    print("[INFO] Initial Data Quality Checks for Feature Data:")
    print(f"Number of records: {df.count()}")
    print(f"Number of duplicate records: {df.count() - df.dropDuplicates().count()}")

    # Count null values before cleaning
    null_counts_before = df.select([count(when(col(c).isNull() | (col(c) == "NA") | (col(c) == "na"), c)).alias(c) for c in df.columns])
    print("[INFO] Null Values Count Before Cleaning:")
    null_counts_before.show()

    # Display DataFrame before cleaning
    print("[INFO] Feature Data Before Cleaning (with null values):")
    df.show()

    # Replace "NA" or "na" with null values
    df = df.replace("NA", None).replace("na", None)

    # Drop rows with null values
    df = df.dropna()

    # Final checks after cleaning
    print("[INFO] Data Quality Checks After Cleaning for Feature Data:")
    print(f"Number of records: {df.count()}")
    print(f"Number of duplicate records: {df.count() - df.dropDuplicates().count()}")

    # Count null values after cleaning
    null_counts_after = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    print("[INFO] Null Values Count After Cleaning:")
    null_counts_after.show()

    # Display DataFrame after cleaning
    print("[INFO] Feature Data After Cleaning (without null values):")
    df.show()

    return df

# Function to clean other data (handle missing values)
def clean_other_data(df):
    # Initial counts
    print("[INFO] Initial Data Quality Checks for Other Data:")
    print(f"Number of records: {df.count()}")
    print(f"Number of duplicate records: {df.count() - df.dropDuplicates().count()}")

    # Count null values before cleaning
    null_counts_before = df.select([count(when(col(c).isNull() | (col(c) == "NA") | (col(c) == "na"), c)).alias(c) for c in df.columns])
    print("[INFO] Null Values Count Before Cleaning:")
    null_counts_before.show()

    # Display DataFrame before cleaning
    print("[INFO] Other Data Before Cleaning (with null values):")
    df.show()

    # Replace "NA" or "na" with null values
    df = df.replace("NA", None).replace("na", None)
    
    # Remove duplicates
    df = df.dropDuplicates()

    # Handle missing values for numeric columns (int, float)
    numeric_cols = [field.name for field in df.schema.fields if field.dataType in [FloatType(), "IntegerType", "DoubleType"]]
    for col_name in numeric_cols:
        mean_val = df.select(mean(col(col_name))).collect()[0][0]
        df = df.withColumn(col_name, when(col(col_name).isNull(), mean_val).otherwise(col(col_name)))

    # Handle missing values for categorical columns (string)
    string_cols = [field.name for field in df.schema.fields if field.dataType == StringType()]
    for col_name in string_cols:
        mode_val = df.groupBy(col_name).count().orderBy("count", ascending=False).first()[0]
        df = df.withColumn(col_name, when(col(col_name).isNull(), mode_val).otherwise(col(col_name)))

    # Handle missing values in 'Date' column and convert to proper format
    if "Date" in df.columns:
        df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))
        # Optionally, if null date needs special handling (forward fill or drop):
        window = Window.orderBy("Date")
        df = df.withColumn("Date", when(col("Date").isNull(), first("Date", True).over(window)).otherwise(col("Date")))

    # Final checks after cleaning
    print("[INFO] Data Quality Checks After Cleaning for Other Data:")
    print(f"Number of records: {df.count()}")
    print(f"Number of duplicate records: {df.count() - df.dropDuplicates().count()}")

    # Count null values after cleaning
    null_counts_after = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    print("[INFO] Null Values Count After Cleaning:")
    null_counts_after.show()

    # Display DataFrame after cleaning
    print("[INFO] Other Data After Cleaning (without null values):")
    df.show()

    return df

# Save the cleaned DataFrame back to HDFS
def save_to_hdfs(df, hdfs_path):
    if df and df.count() > 0:
        # Delete existing files in the HDFS path before saving
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(hdfs_path), True)
        print(f"[INFO] Deleted existing files at HDFS path: {hdfs_path}")

        # Save the new records to HDFS
        df.write.mode("overwrite").option("header", "true").csv(hdfs_path)
        print(f"[INFO] Records saved to HDFS at {hdfs_path}")
    else:
        print("[INFO] No new records to save to HDFS.")

# Define paths for HDFS for both incremental load and full load
files_to_clean = [
    {
        "incremental_path": "/tmp/david/incremental_load/new_sales/part-00000-*.csv", 
        "full_load_path": "/tmp/david/full_load/new_sales/part-00000-*.csv", 
        "save_path": "/tmp/david/curated_data/new_sales",
        "cleaning_function": clean_other_data  # Use the function to clean non-feature files
    },
    {
        "incremental_path": "/tmp/david/incremental_load/features/part-00000-*.csv", 
        "full_load_path": "/tmp/david/full_load/features/part-00000-*.csv", 
        "save_path": "/tmp/david/curated_data/features",
        "cleaning_function": clean_feature_data  # Use the function to drop nulls for feature file
    },
    {
        "incremental_path": "/tmp/david/incremental_load/store/part-00000-*.csv", 
        "full_load_path": "/tmp/david/full_load/store/part-00000-*.csv", 
        "save_path": "/tmp/david/curated_data/store",
        "cleaning_function": clean_other_data  # Use the function to clean non-feature files
    },
    {
        "incremental_path": "/tmp/david/incremental_load/past_sales/part-00000-*.csv", 
        "full_load_path": "/tmp/david/full_load/past_sales/part-00000-*.csv", 
        "save_path": "/tmp/david/curated_data/past_sales",
        "cleaning_function": clean_other_data  # Use the function to clean non-feature files
    }
]

# Process each file for cleaning, appending, and saving
for file_info in files_to_clean:
    incremental_df = load_data(file_info["incremental_path"])
    cleaned_incremental_df = file_info["cleaning_function"](incremental_df)
    full_load_df = load_data(file_info["full_load_path"])
    appended_df = full_load_df.union(cleaned_incremental_df)
    final_cleaned_df = file_info["cleaning_function"](appended_df)
    save_to_hdfs(final_cleaned_df, file_info["save_path"])

# Stop Spark session
spark.stop()


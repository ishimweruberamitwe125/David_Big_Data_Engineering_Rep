from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, stddev, mean, when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DataCleaningValidation") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .getOrCreate()

# Function to load data from HDFS
def load_data(hdfs_path):
    return spark.read.option("header", "true").csv(hdfs_path)

# Function to validate cleaning
def validate_cleaning(df):
    # Check for null values
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    null_counts.show()

    # Check for duplicates
    duplicate_count = df.count() - df.dropDuplicates().count()
    print(f"Number of duplicates: {duplicate_count}")

    # Check for outliers (using Z-score method)
    numeric_cols = [field.name for field in df.schema.fields if field.dataType == "FloatType"]
    total_outliers = 0
    for col_name in numeric_cols:
        stddev_val = df.select(stddev(col(col_name))).collect()[0][0]
        mean_val = df.select(mean(col(col_name))).collect()[0][0]
        outliers_in_col = df.filter(((col(col_name) - mean_val) / stddev_val).between(-3, 3) == False).count()
        total_outliers += outliers_in_col
        print(f"Number of outliers in {col_name}: {outliers_in_col}")

    print(f"Total number of outliers: {total_outliers}")

    # Validate that there are no duplicates or outliers left
    if duplicate_count == 0 and total_outliers == 0:
        print("[INFO] Data validation passed: No duplicates or outliers.")
    else:
        print("[ERROR] Data validation failed: Issues found with duplicates or outliers.")

# Define path for testing cleaned data
hdfs_path = "/tmp/david/curated_data/new_sales"
df = load_data(hdfs_path)

# Run validation checks
validate_cleaning(df)

# Stop Spark session
spark.stop()


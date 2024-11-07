from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_timestamp, col, to_utc_timestamp
from datetime import datetime

# Create Spark session with PostgreSQL JDBC configuration and HDFS
spark = SparkSession.builder \
    .appName("PostgresToHDFSIncremental") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .config("spark.jars", "/usr/local/lib/postgresql-42.2.18.jar") \
    .getOrCreate()

# PostgreSQL connection details
jdbc_url = "jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb"
connection_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Function to get the latest 'last_modified' timestamp from PostgreSQL before inserting new records
def get_min_timestamp(table_name):
    query = f"(SELECT MAX(last_modified) as last_timestamp FROM {table_name}) as last_record_info"
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
    last_timestamp = df.collect()[0]["last_timestamp"]
    print(f"[INFO] Table '{table_name}' has the latest 'last_modified' timestamp: {last_timestamp}")
    return last_timestamp

# Function to insert up to 100 new records into PostgreSQL and add 'last_modified' column with UTC timestamp
def insert_new_records_from_file(file_path, table_name):
    print(f"[INFO] Inserting new records into table '{table_name}'...")

    # Load the data from the CSV file (use local file system path)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"file://{file_path}")

    # If the 'Date' column exists, cast it to a timestamp type to avoid type mismatch issues
    if "Date" in df.columns:
        df = df.withColumn("Date", to_timestamp(col("Date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))  # Cast to proper timestamp format

    # Insert the first 100 records from the file into PostgreSQL with a new 'last_modified' timestamp
    new_records_to_insert = df.limit(100).withColumn("last_modified", to_utc_timestamp(current_timestamp(), "UTC"))
    new_records_to_insert.write \
        .mode("append") \
        .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

    print(f"[INFO] Inserted 100 new records into the table '{table_name}'.")

    return new_records_to_insert

# Function to fetch exactly 100 newly inserted records from PostgreSQL after insertion
def fetch_new_records_from_db(table_name, min_last_modified, inserted_records_count=100):
    print(f"[INFO] Fetching up to {inserted_records_count} newly inserted records from table '{table_name}'...")

    # Ensure the correct formatting of min timestamp for PostgreSQL
    min_last_modified_str = min_last_modified.strftime('%Y-%m-%d %H:%M:%S')

    print(f"[DEBUG] Fetching records with 'last_modified' greater than '{min_last_modified_str}'")

    # Define query to fetch new records from PostgreSQL based on 'last_modified', limit to inserted count
    query = f"(SELECT * FROM {table_name} WHERE last_modified > '{min_last_modified_str}' ORDER BY last_modified ASC LIMIT {inserted_records_count}) as new_records"
    
    # Load the new data from PostgreSQL
    new_records_df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
    record_count = new_records_df.count()
    print(f"[INFO] Fetched {record_count} new records from table '{table_name}'.")

    return new_records_df if record_count > 0 else None

# Function to save only the newly inserted records to HDFS
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

# Function to handle the entire process for each table
def process_table_and_file(table_name, file_path, hdfs_path_base):
    # Step 1: Get the latest timestamp before inserting new records
    min_last_modified = get_min_timestamp(table_name)

    # Step 2: Insert 100 new records from the file into the table
    insert_new_records_from_file(file_path, table_name)

    # Step 3: Fetch exactly 100 newly inserted records from the table
    new_records_df = fetch_new_records_from_db(table_name, min_last_modified, 100)

    # Step 4: If new records were fetched, save them to HDFS
    if new_records_df is not None:
        hdfs_path = f"{hdfs_path_base}/{table_name}"
        save_to_hdfs(new_records_df, hdfs_path)

# Define paths and parameters for multiple tables/files
hdfs_path_base = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022/tmp/david/incremental_load"

# Define the table-file mappings (use local file paths here)
tables_and_files = [
    {"table_name": "features", "file_path": "/home/ec2-user/UKUSSeptBatch/david/features.csv"},
    {"table_name": "store", "file_path": "/home/ec2-user/UKUSSeptBatch/david/stores.csv"},
    {"table_name": "past_sales", "file_path": "/home/ec2-user/UKUSSeptBatch/david/train.csv"},
    {"table_name": "new_sales", "file_path": "/home/ec2-user/UKUSSeptBatch/david/test.csv"}
]

# Process each table and corresponding file
for mapping in tables_and_files:
    process_table_and_file(mapping["table_name"], mapping["file_path"], hdfs_path_base)

# Stop the Spark session
spark.stop()


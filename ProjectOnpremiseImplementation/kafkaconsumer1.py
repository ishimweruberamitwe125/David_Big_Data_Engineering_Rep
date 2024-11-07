from kafka import KafkaConsumer, TopicPartition
import psycopg2
import json
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.types as T
from datetime import datetime

# PostgreSQL connection details
conn = psycopg2.connect(
    host="ec2-18-132-73-146.eu-west-2.compute.amazonaws.com",
    port="5432",
    dbname="testdb",
    user="consultants",
    password="WelcomeItc@2022"
)
cursor = conn.cursor()

# Create table in PostgreSQL if not exists
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales_changes (
        store_id INT,
        dept INT,
        date DATE,
        weekly_sales FLOAT,
        isholiday BOOLEAN,
        last_modified TIMESTAMP
    );
""")
conn.commit()

# Kafka Consumer configuration
consumer = KafkaConsumer(
    bootstrap_servers=[
        'ip-172-31-13-101.eu-west-2.compute.internal:9092',
        'ip-172-31-3-80.eu-west-2.compute.internal:9092',
        'ip-172-31-5-217.eu-west-2.compute.internal:9092',
        'ip-172-31-9-237.eu-west-2.compute.internal:9092'
    ],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Assign the consumer to partition 0 of the topic 'past_sales_changes1'
partition = TopicPartition('past_sales_changes1', 0)
consumer.assign([partition])

# Set the consumer to start from offset 0
consumer.seek(partition, 0)

# Initialize Spark session for HDFS
spark = SparkSession.builder \
    .appName("SalesChangesConsumer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .getOrCreate()

# Buffer to store updated records before saving them in bulk
updated_records = []

# Function to insert data into PostgreSQL
def store_to_postgresql(record):
    query = """
        INSERT INTO sales_changes (store_id, dept, date, weekly_sales, isholiday, last_modified)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.execute(query, (
        record['Store'], 
        record['Dept'], 
        record['Date'], 
        record['Weekly_Sales'], 
        record['IsHoliday'], 
        record['last_modified']
    ))
    conn.commit()

# Function to parse date and timestamp strings properly
def parse_record(record):
    # Convert the ISO8601 timestamps and dates to Python datetime
    record['Date'] = datetime.strptime(record['Date'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
    record['last_modified'] = datetime.strptime(record['last_modified'], '%Y-%m-%dT%H:%M:%S.%fZ')
    return record

# Function to save all records to HDFS at once when the user stops the consumer
def save_all_to_hdfs(records):
    if len(records) > 0:
        # Define schema to ensure consistent structure
        schema = T.StructType([
            T.StructField("Store", T.IntegerType(), True),
            T.StructField("Dept", T.IntegerType(), True),
            T.StructField("Date", T.DateType(), True),
            T.StructField("Weekly_Sales", T.FloatType(), True),
            T.StructField("IsHoliday", T.BooleanType(), True),
            T.StructField("last_modified", T.TimestampType(), True)
        ])

        # Convert records to a list of Row objects (required for DataFrame creation)
        rows = [Row(**rec) for rec in records]

        # Convert rows to a Spark DataFrame with schema validation
        df = spark.createDataFrame(rows, schema=schema)

        # Write to HDFS (overwrite mode, saving all at once)
        df.write.mode('overwrite').json('/tmp/david/curated_data/')
        print(f"Saved {len(records)} records to HDFS.")

# Start consuming messages from Kafka
try:
    for message in consumer:
        record = message.value

        if isinstance(record, str):
            record = json.loads(record)

        # Parse the record to ensure correct date and timestamp formats
        parsed_record = parse_record(record)

        print(f"Received updated record: {parsed_record}")

        # Store the record in PostgreSQL
        store_to_postgresql(parsed_record)

        # Add the parsed record to the buffer
        updated_records.append(parsed_record)

except KeyboardInterrupt:
    # When the user cancels (presses Ctrl+C), display and save all updated records
    print("Consumer stopped by user. Displaying all updated records:")

    # Display each updated record
    for record in updated_records:
        print(record)

    # Save all updated records to HDFS when user stops the consumer
    save_all_to_hdfs(updated_records)

finally:
    # Clean up resources
    cursor.close()
    conn.close()
    consumer.close()
    spark.stop()


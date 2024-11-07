import json
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as sql_max
from kafka import KafkaProducer

# PostgreSQL connection details (including database name)
jdbc_url = "jdbc:postgresql://ec2-18-132-73-146.eu-west-2.compute.amazonaws.com:5432/testdb"
connection_properties = {
    "user": "consultants",
    "password": "WelcomeItc@2022",
    "driver": "org.postgresql.Driver"
}

# Kafka configuration
kafka_topic = "past_sales_changes1"
bootstrap_servers = [
    "ip-172-31-13-101.eu-west-2.compute.internal:9092",
    "ip-172-31-3-80.eu-west-2.compute.internal:9092",
    "ip-172-31-5-217.eu-west-2.compute.internal:9092",
    "ip-172-31-9-237.eu-west-2.compute.internal:9092"
]

# Initialize Spark session for HDFS
spark = SparkSession.builder \
    .appName("RealTimeSalesMonitoring") \
    .config("spark.jars", "/usr/local/lib/postgresql-42.2.18.jar") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8022") \
    .getOrCreate()

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to store last_checked timestamp
def save_last_checked_timestamp(timestamp):
    with open('last_checked.txt', 'w') as f:
        f.write(timestamp)

# Function to retrieve the last_checked timestamp
def load_last_checked_timestamp():
    # If the file doesn't exist, use a default timestamp (to limit first-time retrieval)
    if os.path.exists('last_checked.txt'):
        with open('last_checked.txt', 'r') as f:
            return f.read().strip()
    return "2024-10-22 00:00:00"  # Set the starting time of interest

# Function to get the latest changes from the past_sales table
def get_changes(last_checked):
    # Fetch only records with last_modified greater than the last checked timestamp
    query = f"(SELECT * FROM past_sales WHERE last_modified > '{last_checked}' ORDER BY last_modified) AS new_sales"
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)
    return df

# Function to send changes to Kafka
def send_to_kafka(df):
    if df.count() > 0:
        # Convert DataFrame to JSON and send to Kafka
        data = df.toJSON().collect()
        for record in data:
            producer.send(kafka_topic, value=record)
            print(f"Record sent to Kafka: {record}")
        print(f"Sent {df.count()} records to Kafka topic {kafka_topic}")

# Main logic
try:
    while True:
        # Load the last checked timestamp
        last_checked = load_last_checked_timestamp()

        # Get the latest changes since the last checked timestamp
        changes_df = get_changes(last_checked)

        # Send the updated records to Kafka
        send_to_kafka(changes_df)

        # Update the last_checked timestamp to the most recent 'last_modified' value
        if changes_df.count() > 0:
            new_last_checked = changes_df.agg(sql_max("last_modified")).collect()[0][0]
            save_last_checked_timestamp(str(new_last_checked))

        # Sleep for 5 seconds before the next check
        time.sleep(5)

except KeyboardInterrupt:
    print("Producer stopped by user.")

finally:
    # Clean up resources
    producer.close()
    spark.stop()


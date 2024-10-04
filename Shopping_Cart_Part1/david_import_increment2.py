#writer:David Iahimwe Ruberamitwe
import psycopg2
import csv
import os
import subprocess
import platform
# PostgreSQL connection details
DB_HOST = "ec2-18-132-73-146.eu-west-2.compute.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "testdb"
DB_USER = "consultants"
DB_PASSWORD = "WelcomeItc@2022"
if platform.system() == "Windows":# Paths depending on the platform
    LOCAL_FILE_PATH = r"C:\Users\David Ruberamitwe\Downloads\daproducts1.csv"# Windows paths
else:
    LOCAL_FILE_PATH = None  # No local file on Linux, will be using HDFS
    HDFS_FILE_PATH = r"ukussept/david/david_product/daproducts1.csv"# HDFS path 
last_value_in_memory = 0 # Store last_value in memory
def get_last_value():# Function to get the last imported value of product_id from memory
    global last_value_in_memory
    return last_value_in_memory

def save_last_value(last_value):# Function to save the last imported value of product_id in memory
    global last_value_in_memory
    last_value_in_memory = last_value
def upload_to_hdfs(hdfs_path):# Function to upload the file to HDFS only used in Linux
    try:
        subprocess.run(["hdfs", "dfs", "-rm", hdfs_path], check=False)# Remove existing file in HDFS to avoid duplicates
        subprocess.run(["hdfs", "dfs", "-put", '/tmp/temp_daproducts.csv', hdfs_path], check=True)# Upload the CSV file to HDFS
        print(f"File successfully uploaded to HDFS at {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading file to HDFS: {e}")
# Step 1: Full Load (Import All Records)
def full_load():
    try:# Ensure the output directory exists for Windows only
        if platform.system() == "Windows":
            output_directory = os.path.dirname(LOCAL_FILE_PATH)
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)  # Create the directory if it doesn't exist
        connection = psycopg2.connect(# Connect to PostgreSQL
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()
        last_value = get_last_value()# Fetch the last imported product_id
        print(f"Last imported product_id: {last_value}")
        # Fetch new records from PostgreSQL
        query = f"SELECT DISTINCT product_id, name, category, effective_date, last_modified FROM products WHERE product_id > {last_value} ORDER BY product_id ASC"
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            print("No new records to import.")
        else:
            colnames = [desc[0] for desc in cursor.description]# Get the column names
            if platform.system() == "Windows":
                # Windows: Write data to a CSV file
                with open(LOCAL_FILE_PATH, mode='a', newline='', encoding='utf-8') as csv_file:
                    writer = csv.writer(csv_file)
                    # Only write header if the file is empty or being created for the first time
                    if os.path.getsize(LOCAL_FILE_PATH) == 0:
                        writer.writerow(colnames)  # Writing column names
                    writer.writerows(rows)  # Writing the new data
                print(f"Data successfully appended to {LOCAL_FILE_PATH}")
            else:
                with open('/tmp/temp_daproducts.csv', mode='w', newline='', encoding='utf-8') as csv_file:# Linux: Upload data to HDFS
                    writer = csv.writer(csv_file)
                    writer.writerow(colnames)  # Writing column names
                    writer.writerows(rows)  # Writing the new data
                upload_to_hdfs(HDFS_FILE_PATH)# Move the temporary file to HDFS
            # Update last_value with the last product_id from the fetched rows
            last_imported_id = rows[-1][0]  # Assuming 'product_id' is the first column
            save_last_value(last_imported_id)
            print(f"Last value updated to: {last_imported_id}")
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL or writing to CSV: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed.")
def add_new_records():# Step 2: Insert New Records
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()
        # Insert new records
        insert_query = """
        INSERT INTO products (product_id, name, category, effective_date)
        VALUES 
        (%s, %s, %s, %s),
        (%s, %s, %s, %s);
        """
        new_data = (
            118, 'Product F', 'Clothing', '2023-07-01',  # First record
            119, 'Product G', 'Electronics', '2023-07-02'  # Second record
        )
        cursor.execute(insert_query, new_data)
        # Commit the transaction
        connection.commit()
        print("New records inserted into PostgreSQL successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error while inserting into PostgreSQL: {error}")
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed.")
def incremental_load():# Step 3: Incremental Load (I Set last_value to 117 due to that was the last value of the data at the table and loads new data)
    # Set the last imported value to 117 manually
    save_last_value(117)
    # Perform full load based on the last imported value
    full_load()
# Execute the steps
def main():
    if platform.system() == "Windows":
        print("Step 1: Full Load (Windows)")
        full_load()
        print("\nStep 2: Adding New Records")
        add_new_records()
        print("\nStep 3: Incremental Load (Windows)")
        incremental_load()
    else:
        print("Step 1: Full Load (Hadoop)")
        full_load()
        print("\nStep 2: Adding New Records")
        add_new_records()
        print("\nStep 3: Incremental Load (Hadoop)")
        incremental_load()
if __name__ == "__main__":
    main()

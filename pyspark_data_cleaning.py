from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, when, col
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve PostgreSQL connection details from environment variables
staging_db_name = os.getenv('staging_db_name')
staging_db_user = os.getenv('staging_db_user')
staging_db_password = os.getenv('staging_db_password')
staging_db_host = os.getenv('staging_db_host')
staging_db_port = os.getenv('staging_db_port')

print(f"DB Host: {staging_db_host}")

# Create a Spark session
spark = SparkSession \
        .builder \
        .appName("Data Cleaning Staging Area") \
        .getOrCreate()

jdbc_url = f"jdbc:postgresql://{staging_db_host}:{staging_db_port}/{staging_db_name}"
connection_properties = {
    "user": staging_db_user,
    "password": staging_db_password,
    "driver": "org.postgresql.Driver"
}

# Function to check if table exists
def table_exists(table_name):
    try:
        check_table_query = f"(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}') AS table_check"
        table_exists_df = spark.read.jdbc(url=jdbc_url, table=check_table_query, properties=connection_properties)
        return table_exists_df.count() > 0
    except Exception as e:
        print(f"Error checking if table exists: {e}")
        return False

# Function to get maximum orderid from the table if it exists
def get_max_orderid():
    if table_exists('sales_staging_cleaned'):
        max_orderid_df = spark.read.jdbc(url=jdbc_url, table="(SELECT MAX(orderid) AS max_orderid FROM public.sales_staging_cleaned) AS max_orderid_table", properties=connection_properties)
        if max_orderid_df.count() > 0:
            max_orderid = max_orderid_df.collect()[0]['max_orderid']
            print(f"Max orderid found: {max_orderid}")
            return max_orderid if max_orderid is not None else 0
        else:
            print("No records found in sales_staging_cleaned.")
            return 0
    else:
        print("Table sales_staging_cleaned does not exist.")
        return 0

# Get the maximum orderid from the cleaned table
max_orderid = get_max_orderid()

# Read new data from PostgreSQL table sales_staging with limit of 10000
query = f"(SELECT * FROM public.sales_staging WHERE orderid > {max_orderid} ORDER BY orderid LIMIT 10000) AS new_data"
sdf = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# Check if the DataFrame is empty
if sdf.isEmpty():
    print("No new data to clean.")
    spark.stop()
    exit(0)  # Exit the script
else:
    # Show the first few rows upon successful read
    sdf.show(5)

    # Verify the minimum orderid in the new data
    min_orderid = sdf.agg({"orderid": "min"}).collect()[0][0]
    print(f"Minimum orderid in the new data: {min_orderid}")

    # Clean up the "country" column by removing numbers
    sdf = sdf.withColumn("country", regexp_replace("country", "\\d+", ""))

    # Map weekdays and quarters
    weekday_mapping = {
        1: "Sunday",
        2: "Monday",
        3: "Tuesday",
        4: "Wednesday",
        5: "Thursday",
        6: "Friday",
        7: "Saturday"
    }

    quarter_mapping = {
        1: "Q1",
        2: "Q2",
        3: "Q3",
        4: "Q4"
    }

    # Fill missing values in "weekdayname" based on "weekday"
    sdf = sdf.withColumn("weekdayname", 
                         when(col("weekdayname").isNull() | (col("weekdayname") == ""), col("weekday").cast("string"))
                         .otherwise(col("weekdayname")))

    # Replace numeric weekday values with their string equivalents
    for k, v in weekday_mapping.items():
        sdf = sdf.withColumn("weekdayname", 
                             when(col("weekdayname") == str(k), v)
                             .otherwise(col("weekdayname")))

    # Fill missing values in "quartername" based on "quarter"
    sdf = sdf.withColumn("quartername", 
                         when(col("quartername").isNull() | (col("quartername") == ""), col("quarter").cast("string"))
                         .otherwise(col("quartername")))

    # Replace numeric quarter values with their string equivalents
    for k, v in quarter_mapping.items():
        sdf = sdf.withColumn("quartername", 
                             when(col("quartername") == str(k), v)
                             .otherwise(col("quartername")))

    # Create the cleaned dataframe
    output_df = sdf

    # Check if the table exists, and write the result from the cleaned dataframe to the 'sales_staging_cleaned' table in PostgreSQL
    if table_exists('sales_staging_cleaned'):
        output_df.write.jdbc(url=jdbc_url, table="public.sales_staging_cleaned", mode="append", properties=connection_properties)
    else:
        print("Table sales_staging_cleaned does not exist. Please create it manually.")

# Stop the SparkSession
spark.stop()

# Create a checkpoint file to signal that data is ready for data transfer (pyspark_data_transfer.py)
with open("/home/ruddy/spark-cluster/cleaned_batch_ready", "w") as f:
    f.write("Batch 1 cleaned data ready")

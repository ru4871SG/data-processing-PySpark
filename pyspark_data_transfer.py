'''
Pyspark script to transfer data from staging area to production database, and store it properly in the dimension and fact tables.
'''


from pyspark.sql import SparkSession
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

production_db_name = os.getenv('production_db_name')
production_db_user = os.getenv('production_db_user')
production_db_password = os.getenv('production_db_password')
production_db_host = os.getenv('production_db_host')
production_db_port = os.getenv('production_db_port')

# Create a Spark session
spark = SparkSession \
        .builder \
        .appName("Data Transfer from Staging to Production") \
        .getOrCreate()

staging_jdbc_url = f"jdbc:postgresql://{staging_db_host}:{staging_db_port}/{staging_db_name}"
production_jdbc_url = f"jdbc:postgresql://{production_db_host}:{production_db_port}/{production_db_name}"

staging_connection_properties = {
    "user": staging_db_user,
    "password": staging_db_password,
    "driver": "org.postgresql.Driver"
}

production_connection_properties = {
    "user": production_db_user,
    "password": production_db_password,
    "driver": "org.postgresql.Driver"
}

# Function to check if table exists in the production database
def table_exists(table_name):
    try:
        check_table_query = f"(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '{table_name}') AS table_check"
        table_exists_df = spark.read.jdbc(url=production_jdbc_url, table=check_table_query, properties=production_connection_properties)
        return table_exists_df.count() > 0
    except Exception as e:
        print(f"Error checking if table exists: {e}")
        return False

# Function to get max orderid from the fact_orders table in the production database if it exists
def get_max_orderid():
    if table_exists('fact_orders'):
        max_orderid_df = spark.read.jdbc(url=production_jdbc_url, table="(SELECT MAX(orderid) AS max_orderid FROM public.fact_orders) AS max_orderid_table", properties=production_connection_properties)
        if max_orderid_df.count() > 0:
            max_orderid = max_orderid_df.collect()[0]['max_orderid']
            print(f"Max orderid found in fact_orders: {max_orderid}")
            return max_orderid if max_orderid is not None else 0
        else:
            print("No records found in fact_orders.")
            return 0
    else:
        print("Table fact_orders does not exist.")
        return 0

# Get the maximum orderid from the fact_orders table in the production database
max_orderid = get_max_orderid()

# Read new data from sales_staging_cleaned with the limit of 10000, and orderid greater than the max orderid from fact_orders
query = f"(SELECT * FROM public.sales_staging_cleaned WHERE orderid > {max_orderid} ORDER BY orderid LIMIT 10000) AS new_data"
sdf = spark.read.jdbc(url=staging_jdbc_url, table=query, properties=staging_connection_properties)

if sdf.isEmpty():
    print("No new data to transfer.")
    spark.stop()
    exit(0)  # Exit the script
else:
    sdf.show(5)

    # Read existing IDs from the dimension tables in the production database
    existing_dateids_df = spark.read.jdbc(url=production_jdbc_url, table="(SELECT dateid FROM public.dim_date) AS existing_dateids", properties=production_connection_properties)
    existing_categoryids_df = spark.read.jdbc(url=production_jdbc_url, table="(SELECT categoryid FROM public.dim_category) AS existing_categoryids", properties=production_connection_properties)
    existing_countryids_df = spark.read.jdbc(url=production_jdbc_url, table="(SELECT countryid FROM public.dim_country) AS existing_countryids", properties=production_connection_properties)

    existing_dateids = [row.dateid for row in existing_dateids_df.collect()]
    existing_categoryids = [row.categoryid for row in existing_categoryids_df.collect()]
    existing_countryids = [row.countryid for row in existing_countryids_df.collect()]

    # Filter out records with duplicate IDs for the dimension tables
    dim_date_df = sdf.select("dateid", "date", "year", "quarter", "quartername", "month", "monthname", "day", "weekday", "weekdayname").dropDuplicates(["dateid"])
    dim_date_df = dim_date_df.filter(~dim_date_df.dateid.isin(existing_dateids))

    dim_category_df = sdf.select("categoryid", "category").dropDuplicates(["categoryid"])
    dim_category_df = dim_category_df.filter(~dim_category_df.categoryid.isin(existing_categoryids))

    dim_country_df = sdf.select("countryid", "country").dropDuplicates(["countryid"])
    dim_country_df = dim_country_df.filter(~dim_country_df.countryid.isin(existing_countryids))

    # For the fact_orders table, while we still filter out records with duplicate order IDs, no need to check existing records since we already have max_orderid
    fact_orders_df = sdf.select("orderid", "dateid", "countryid", "categoryid", "amount").dropDuplicates(["orderid"])

    # Insert new records to all the tables
    if not dim_date_df.isEmpty():
        dim_date_df.write.jdbc(url=production_jdbc_url, table="public.dim_date", mode="append", properties=production_connection_properties)

    if not dim_category_df.isEmpty():
        dim_category_df.write.jdbc(url=production_jdbc_url, table="public.dim_category", mode="append", properties=production_connection_properties)

    if not dim_country_df.isEmpty():
        dim_country_df.write.jdbc(url=production_jdbc_url, table="public.dim_country", mode="append", properties=production_connection_properties)

    if not fact_orders_df.isEmpty():
        fact_orders_df.write.jdbc(url=production_jdbc_url, table="public.fact_orders", mode="append", properties=production_connection_properties)

    print("Data transfer completed.")

# Stop the SparkSession
spark.stop()

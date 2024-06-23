# Data Processing with PySpark

This repository contains scripts that can be submitted to the Spark cluster for a data pipeline that involves multiple tasks (data cleaning, data transfer, and clustering using Spark ML). The processed data is then stored in PostgreSQL production database.

## Project Structure

- `pyspark_data_cleaning.py`: This is a PySpark script that you can submit to the Spark cluster to automate the data cleaning process in the staging area. It will extract the data from `sales_staging` table, clean it, and store the cleaned result in `sales_staging_cleaned` table. Every time this script is executed, it will process 10,000 rows. This script is able to detect the last processed orderid from `sales_staging_cleaned` table. 

- `pyspark_data_transfer.py`: This is a PySpark script that transfers the cleaned data from `sales_staging_cleaned` table in the staging area, and store it to the correct dimension and fact tables in the production database. Every time this script is submitted to the Spark cluster, it will transfer 10,000 rows. This script is able to detect the last transferred orderid by checking the fact table in the production database.  

- `submit_spark_job_cleaning_and_transfer.sh`: This Bash script automatically submits `pyspark_data_cleaning.py` and `pyspark_data_transfer.py` to the Spark cluster, and since both PySpark scripts processes 10,000 rows at a time, they will automatically be resubmitted to the Spark cluster to process the next 10,000 rows. This cycle continues until the entire 300,000 rows of data have been both cleaned and transferred.

- `pyspark_clustering.py`: This PySpark script performs Clustering using Spark ML and stores the result in a new table in the production database.

- `submit_spark_job_clustering.sh`: This Bash script submits `pyspark_clustering.py` to the Spark cluster.

- `01_create_staging_area.sql` and `02_create_production_db_star_schema.sql`: These SQL scripts are used to set up the database schema in PostgreSQL.

- `source/sales_data.csv`: This is the source data file that you can import to the original table in the staging area, which will be cleaned by `pyspark_data_cleaning.py`.

## How to Use

1. Set up your PostgreSQL databases by manually executing the SQL scripts `01_create_staging_area.sql` and `02_create_production_db_star_schema.sql`. Make sure to separate the databases for `01_create_staging_area.sql` and `02_create_production_db_star_schema.sql`. The first SQL script is for the staging area while the second SQL script is to build a star schema in production database. 

2. Update the `.env` file with your PostgreSQL connection details. You can refer to `.env.example` for the structure.

3. Import the included csv file `source/sales_data.csv` into the original table `sales_staging` in the staging area. You can use PgAdmin to perform this import process.

4. Feel free to change the configuration in `docker-compose.yaml` if your Spark cluster manager is different. I use Spark Standalone Cluster on a local machine, which is running on Docker Compose.

5. Once your Spark cluster manager is up and running, you can run the Bash script `submit_spark_job_cleaning_and_transfer.sh`to submit two PySpark scripts (`pyspark_data_cleaning.py` and `pyspark_data_transfer.py`) to the cluster. Since both PySpark scripts processes 10,000 rows at a time, they will automatically be resubmitted to the Spark cluster to process the next 10,000 rows. This cycle continues until the entire 300,000 rows of data have been both cleaned and transferred.

6. After all the data has been transferred to production database, you can run the Bash script `submit_spark_job_clustering.sh` to submit the last PySpark script `pyspark_clustering.py`, which will perform Clustering using Spark ML. The result will be stored in a new table in the production database. 

Please note that you need to have Spark and PostgreSQL installed.
#!/bin/bash

# Define the path to the local spark-submit folder
SPARK_SUBMIT_PATH="/home/ruddy/spark-cluster/spark-env/bin/spark-submit"

# Define the path to the PySpark script
SCRIPT_PATH="$(dirname "$0")/clustering_postgre.py"

# Path to the PostgreSQL JDBC driver
JDBC_DRIVER_PATH="/home/ruddy/jdbc/postgresql-jdbc.jar"

# Submit Spark job
$SPARK_SUBMIT_PATH \
    --master spark://localhost:7077 \
    --conf spark.driver.memory=4g \
    --conf spark.executor.memory=1536m \
    --conf spark.total.executor.cores=2 \
    --conf spark.executor.cores=1 \
    --jars $JDBC_DRIVER_PATH \
    --name "Data Processing Demonstration" \
    $SCRIPT_PATH

echo "Spark clustering job submitted successfully."

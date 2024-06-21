#!/bin/bash

# This Bash script will submit pyspark_data_cleaning.py and pyspark_data_transfer.py to the Spark cluster, and they will run side by side until
# all data has been processed.

# Define the path to the local spark-submit folder
SPARK_SUBMIT_PATH="/home/ruddy/spark-cluster/spark-env/bin/spark-submit"

# Define the paths to the PySpark scripts
CLEANING_SCRIPT_PATH="$(dirname "$0")/pyspark_data_cleaning.py"
TRANSFER_SCRIPT_PATH="$(dirname "$0")/pyspark_data_transfer.py"

# Path to the PostgreSQL JDBC driver
JDBC_DRIVER_PATH="/home/ruddy/jdbc/postgresql-jdbc.jar"

# Define the checkpoint file
CHECKPOINT_FILE="//home/ruddy/spark-cluster/cleaned_batch_ready"

# Submit Spark job
submit_spark_job() {
    local script_path=$1
    $SPARK_SUBMIT_PATH \
        --master spark://localhost:7077 \
        --conf spark.driver.memory=4g \
        --conf spark.executor.memory=1536m \
        --conf spark.total.executor.cores=2 \
        --conf spark.executor.cores=1 \
        --jars $JDBC_DRIVER_PATH \
        --name "Data Processing Demonstration" \
        $script_path
}

# Function to check the exit code of the last submitted job
check_exit_code() {
    local exit_code=$1
    if [ $exit_code -ne 0 ]; then
        echo "A Spark job completed with an exit code indicating no new data. Stopping further processing."
        exit 0
    fi
}

while true; do
    # Submit the data cleaning job
    submit_spark_job $CLEANING_SCRIPT_PATH &
    CLEANING_JOB_PID=$!
    wait $CLEANING_JOB_PID
    check_exit_code $?

    # Wait for the checkpoint file to be created (indicating the first batch is ready)
    while [ ! -f $CHECKPOINT_FILE ]; do
        sleep 10  # check every 10 seconds
    done

    # Submit the data transfer job once the checkpoint file is found
    submit_spark_job $TRANSFER_SCRIPT_PATH &
    TRANSFER_JOB_PID=$!
    wait $TRANSFER_JOB_PID
    check_exit_code $?

    # Remove the checkpoint file for future batches
    rm $CHECKPOINT_FILE
done

echo "Both Spark jobs submitted successfully."

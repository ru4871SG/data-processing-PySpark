'''
PySpark script to perform clustering using Spark ML and store the results in the new table.
'''


from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve PostgreSQL connection details from environment variables
production_db_name = os.getenv('production_db_name')
production_db_user = os.getenv('production_db_user')
production_db_password = os.getenv('production_db_password')
production_db_host = os.getenv('production_db_host')
production_db_port = os.getenv('production_db_port')

print(f"DB Host: {production_db_host}")

# Create a Spark session
spark = SparkSession \
        .builder \
        .appName("Clustering in Production") \
        .getOrCreate()

jdbc_url = f"jdbc:postgresql://{production_db_host}:{production_db_port}/{production_db_name}"
connection_properties = {
    "user": production_db_user,
    "password": production_db_password,
    "driver": "org.postgresql.Driver"
}

# Get the data from the fact_orders table
sdf = spark.read.jdbc(url=jdbc_url, table="public.fact_orders", properties=connection_properties)

# Create a VectorAssembler by using "countryid" and "amount" as feature columns
feature_cols = ["countryid", "amount"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Scale the features using Standard Scaler
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

# Create a total of 4 clusters using K-Means
number_of_clusters = 4
kmeans = KMeans(k=number_of_clusters, featuresCol="scaledFeatures")

# Build the pipeline and train the model
pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model = pipeline.fit(sdf)

# Make predictions on the dataset
predictions = model.transform(sdf)

# Select the relevant columns and rename the "prediction" column
output_df = predictions.select("orderid", "countryid", "amount", predictions["prediction"].alias("cluster"))

# Write the result to a new table in PostgreSQL
output_df.write.jdbc(url=jdbc_url, table="public.clustering_result", mode="overwrite", properties=connection_properties)

# Stop the SparkSession
spark.stop()

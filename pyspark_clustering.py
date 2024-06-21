from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
# from pyspark.sql import DataFrameWriter
import os
from dotenv import load_dotenv

load_dotenv()

# Retrieve PostgreSQL connection details from .env file
production_db_name = os.getenv('production_db_name')
production_db_user = os.getenv('production_db_user')
production_db_password = os.getenv('production_db_password')
production_db_host = os.getenv('production_db_host')
production_db_port = os.getenv('production_db_port')

print(f"DB Host: {production_db_host}")

# Create a Spark session
spark = SparkSession \
        .builder \
        .appName("Clustering in Postgre") \
        .getOrCreate()

jdbc_url = f"jdbc:postgresql://{production_db_host}:{production_db_port}/{production_db_name}"
connection_properties = {
    "user": production_db_user,
    "password": production_db_password,
    "driver": "org.postgresql.Driver"
}

sdf = spark.read.jdbc(url=jdbc_url, table="public.fact_orders", properties=connection_properties)

# Show the first few rows upon successful read
sdf.show(5)

# Let's create a VectorAssembler by using "countryid" and "amount" as feature columns
feature_cols = ["countryid", "amount"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Scale the features using Standard Scaler
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

# Create 4 clusters
number_of_clusters = 4

# Create a K-Means clustering model
kmeans = KMeans(k=number_of_clusters, featuresCol="scaledFeatures")

# Build a pipeline
pipeline = Pipeline(stages=[assembler, scaler, kmeans])

# Train the model using the pipeline
model = pipeline.fit(sdf)

# Make predictions on the dataset
predictions = model.transform(sdf)

# Select only the relevant columns to write to the new table
output_df = predictions.select("orderid", "countryid", "amount", "prediction")

# Write the result to a new table in PostgreSQL
output_df.write.jdbc(url=jdbc_url, table="public.clustering", mode="overwrite", properties=connection_properties)

# Stop the SparkSession
spark.stop()

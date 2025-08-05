import os
import psycopg2
from pyspark.sql import SparkSession

# Step 1: Create Spark session with JDBC driver
def create_spark_session():
    return SparkSession.builder \
        .appName("ETL-Load") \
        .config("spark.jars", "jars/postgresql-42.7.3.jar") \
        .getOrCreate()

# Step 2: Load parquet files
def load_transformed_data(spark, transformed_dir):
    artists_df = spark.read.parquet(os.path.join(transformed_dir, "artists"))
    tracks_df = spark.read.parquet(os.path.join(transformed_dir, "tracks"))
    master_df = spark.read.parquet(os.path.join(transformed_dir, "master"))
    return artists_df, tracks_df, master_df

# Step 3: Load data into PostgreSQL
def load_to_postgres(spark, transformed_dir):
    artists_df, tracks_df, master_df = load_transformed_data(spark, transformed_dir)

    jdbc_url = "jdbc:postgresql://localhost:5432/spotifydb"
    db_properties = {
        "user": "postgres",
        "password": "069420",
        "driver": "org.postgresql.Driver"
    }

    # Write data to PostgreSQL
    artists_df.write.jdbc(url=jdbc_url, table="artists", mode="overwrite", properties=db_properties)
    tracks_df.write.jdbc(url=jdbc_url, table="tracks", mode="overwrite", properties=db_properties)
    master_df.write.jdbc(url=jdbc_url, table="master", mode="overwrite", properties=db_properties)

    print("Data loaded to PostgreSQL successfully.")

# Step 4: Main entry
def main():
    spark = create_spark_session()
    transformed_path = os.path.join("transformed_data")
    load_to_postgres(spark, transformed_path)

if __name__ == "__main__":
    main()
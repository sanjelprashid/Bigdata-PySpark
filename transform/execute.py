import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# Step 1: Create a Spark session
def create_spark_session():
    return SparkSession.builder.appName("ETL-Transform").getOrCreate()

# Step 2: Load extracted data and write cleaned parquet files
def load_and_clean_data(spark, input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # Load CSV with inferred schema (fixes column mismatch)
    artists_df = spark.read.option("header", True).csv(os.path.join(input_dir, "artists.csv"))
    tracks_df = spark.read.option("header", True).csv(os.path.join(input_dir, "tracks.csv"))

    # Drop duplicates and nulls
    artists_df = artists_df.dropDuplicates(["id"]).na.drop(subset=["id"])
    tracks_df = tracks_df.dropDuplicates(["id"]).na.drop(subset=["id"])

    # Rename track id to avoid conflict with artist id later
    tracks_df = tracks_df.withColumnRenamed("id", "track_id")

    # Write parquet
    artists_df.write.mode("overwrite").parquet(os.path.join(output_dir, "artists"))
    tracks_df.write.mode("overwrite").parquet(os.path.join(output_dir, "tracks"))

    return artists_df, tracks_df

# Step 3: Create master table
def create_master_table(artists_df, tracks_df, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    # Rename conflicting columns in artists_df to avoid duplicate names
    artists_df = artists_df.withColumnRenamed("name", "artist_name") \
                           .withColumnRenamed("popularity", "artist_popularity")

    # Rename conflicting columns in tracks_df to avoid duplicate names
    tracks_df = tracks_df.withColumnRenamed("name", "track_name") \
                         .withColumnRenamed("popularity", "track_popularity")

    # Explode id_artists column
    tracks_exploded = tracks_df.withColumn("artist_id", explode(split(tracks_df["id_artists"], ",")))

    # Join with artists on artist_id
    master_df = tracks_exploded.join(artists_df, tracks_exploded.artist_id == artists_df.id, "left")

    # Write master parquet
    master_df.write.mode("overwrite").parquet(os.path.join(output_dir, "master"))

# Main execution
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python transform/execute.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()

    artists_df, tracks_df = load_and_clean_data(spark, input_dir, output_dir)
    create_master_table(artists_df, tracks_df, output_dir)

    print("Transformation completed successfully!")
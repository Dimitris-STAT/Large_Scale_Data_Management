from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import from_json, col, broadcast, current_timestamp

# Define schema for Kafka data
songSchema = StructType([
    StructField("id", IntegerType(), False),
    StructField("person_name", StringType(), False),  # Renamed to avoid ambiguity
    StructField("song", StringType(), False)
])

# Create SparkSession
spark = SparkSession.builder \
    .appName("SSKafka") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "test") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert value column to JSON and select columns
sdf = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), songSchema).alias("data")) \
    .select("data.*") \
    .withColumn("time-listened", current_timestamp()) # Add local(PC) time as the time the person
                                                        # listened to the song


# Read spotify_songs as a DataFrame
spotify_songs = spark.read.csv("spotify-songs.csv", header=True)

# Join streaming DataFrame with spotify_songs DataFrame
joined_df = sdf.join(broadcast(spotify_songs), sdf.song == spotify_songs.name, "left")


# Write the streaming DataFrame to the console
query = joined_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Await termination
query.awaitTermination()

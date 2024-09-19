
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType
from pyspark.sql.functions import split,from_json,col, current_timestamp, broadcast, date_format

songSchema = StructType([
                StructField("id", StringType(),False),
                StructField("person_name", StringType(),False),
                StructField("song", StringType(),False)
            ])

spark = SparkSession.builder.appName("SSKafka").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.\
    format("kafka").option("kafka.bootstrap.servers", "localhost:29092")\
    .option("subscribe", "test")\
    .option("startingOffsets", "latest")\
    .load()

sdf = df.selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), songSchema)
    .alias("data")).select("data.*")\
    .withColumn("time_listened", date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')) # Add local(PC) time as the time the person
                                                        # listened to the song

# Read spotify_songs as a DataFrame
spotify_songs = spark.read.csv("spotify-songs.csv", header=True)

# Join streaming DataFrame with spotify_songs DataFrame
joined_df = sdf.join(broadcast(spotify_songs), sdf.song == spotify_songs.name, "left")


def writeToCassandra(writeDF, _):
  writeDF.write.format("org.apache.spark.sql.cassandra")\
      .mode('append')\
      .options(table="music_listening", keyspace="spotify")\
      .save()

result = None
while result is None:
    try:
        # connect
        result = joined_df.writeStream.\
            option("spark.cassandra.connection.host","localhost:9042")\
            .foreachBatch(writeToCassandra).outputMode("update")\
            .start()\
            .awaitTermination()
    except:
         pass


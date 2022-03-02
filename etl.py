import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data"
    
    # read song data file
    df = spark.read.format("json").load("s3://udacity-dend/song_data/*/*/*/*.json")

    # # extract columns to create songs table
    songs_table = df.select(["song_id","title","artist_id","year","duration"])
    
    # # write songs table to parquet files partitioned by year and artist
    song_output_path = output_data + "songs.parquet"
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(song_output_path)

    # # extract columns to create artists table
    artists_table = df.select(\
        col("artist_id"),\
        col("artist_name").alias("name"),\
        col("artist_location").alias("location"),\
        col("artist_latitude").alias("latitude"),\
        col("artist_longitude").alias("longitude"))\
        .dropDuplicates()
    
    artist_output_path = output_data + "artists.parquet"
    # # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(artist_output_path)

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # # read log data file
    df = spark.read.format("json").load(log_data)
    
    # # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # # extract columns for users table    
    users_table = df.select(\
        col("userId").alias("user_id"),\
        col("firstName").alias("first_name"),\
        col("lastName").alias("last_name"),\
        col("gender"),\
        col("level"))\
        .dropDuplicates() 
    
    # # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users.parquet")

    # # create timestamp column from original timestamp column
    df = df.withColumn("start_time", F.to_timestamp(F.from_unixtime(col("ts")/1000), "yyyy-MM-dd HH:mm:ss"))
    
    # # extract columns to create time table
    time_table = df.select(col("start_time")).dropDuplicates()\
        .withColumn("hour", hour("start_time"))\
        .withColumn("day", dayofmonth("start_time"))\
        .withColumn("week", weekofyear("start_time"))\
        .withColumn("month", month("start_time"))\
        .withColumn("year", year("start_time"))\
        .withColumn("weekday", dayofweek("start_time"))
    
    # # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "time.parquet")

    # # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + "songs.parquet")\
        .select(\
            col("song_id"),\
            col("artist_id").alias("artist_id_songs"),\
            col("title"),\
            col("duration")
        )

    artists_df = spark.read.parquet(output_data + "artists.parquet")\
        .select(\
            col("artist_id"),\
            col("name").alias("artist_name")\
        )
    artists_df.show(10)
    # # extract columns from joined song and log datasets to create songplays table 
    songplays_df = df\
        .join(songs_df, [df.song == songs_df.title, df.length == songs_df.duration], "left")\
        .join(artists_df, [df.artist == artists_df.artist_name, songs_df.artist_id_songs == artists_df.artist_id], "left")\
        .select(["start_time","userid","level","song_id","artist_id","sessionid","location"])\
        .withColumnRenamed("userid","user_id")\
        .withColumnRenamed("sessionid","session_id")

    songplays_df = songplays_df.join(time_table.select(["start_time","year","month"]), songplays_df.start_time == time_table.start_time, "left")

    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-udac-dl/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()

if __name__ == "__main__":
    main()

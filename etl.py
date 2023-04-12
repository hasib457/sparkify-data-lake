import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    date_format,
    dayofmonth,
    hour,
    month,
    to_timestamp,
    udf,
    year,
)

config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads in song data from S3, processes it using Spark, and writes the
    songs and artists tables as parquet files partitioned by year and artist.

    Args:
        spark (SparkSession): SparkSession object
        input_data (str): Input data directory path in S3
        output_data (str): Output data directory path in S3
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        "song_id", "title", "artist_id", "year", "duration"
    ).distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table = (
        songs_table.write.partitionBy("year", "artist_id")
        .mode("overwrite")
        .parquet(output_data + "songs/songs_table.parquet")
    )

    # extract columns to create artists table
    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude",
    ).distinct()

    # write artists table to parquet files
    artists_table = artists_table.write.mode("overwrite").parquet(
        output_data + "artists/artists_table.parquet"
    )


def process_log_data(spark, input_data, output_data):
    """
    Reads in log data from S3, processes it using Spark, and writes the
    users and time tables as parquet files. Additionally, it creates a
    songplays table by joining the log and song data, and writes it as parquet
    files partitioned by year and month.

    Args:
        spark (SparkSession): SparkSession object
        input_data (str): Input data directory path in S3
        output_data (str): Output data directory path in S3
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", to_timestamp(df.ts / 1000))

    # create temp view
    df.createOrReplaceTempView("logs")

    # extract columns for users table
    users_table = spark.sql(
        """ SELECT distinct l.userId user_id, firstName first_name, lastName last_name, gender, level
            FROM logs l
            JOIN (
                SELECT userId, MAX(ts) AS max_ts
                FROM logs
                GROUP BY userId
                ) t
            ON l.userId = t.userId AND l.ts = t.max_ts
        """
    )

    # write users table to parquet files
    users_table = users_table.write.mode("overwrite").parquet(
        output_data + "users/users_table.parquet"
    )

    # extract columns to create time table
    time_table = spark.sql(
        """SELECT timestamp as start_time, 
                DATE_PART('hour', timestamp) as hour,
                DATE_PART('day', timestamp) as day,
                DATE_PART('week', timestamp) as week,
                DATE_PART('month', timestamp) as month,
                DATE_PART('year', timestamp) as year,
                DATE_PART('dow', timestamp) as weekday
            FROM logs"""
    )

    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.mode("overwrite").parquet(
        output_data + "time/time_table.parquet"
    )

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(
        """
            SELECT  row_number() OVER (ORDER BY logs.timestamp) songplay_id, logs.timestamp start_time, logs.userId user_id, logs.level, 
                    songs.song_id, songs.artist_id,
                    logs.sessionId session_id, logs.location, logs.userAgent user_agent
            FROM logs 
            JOIN songs  ON (logs.artist= songs.artist_name AND logs.song=songs.title) """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table_with_year_month = songplays_table.withColumn(
        "year", year("start_time")
    ).withColumn("month", month("start_time"))

    songplays_table_with_year_month.write.partitionBy("year", "month").mode(
        "overwrite"
    ).parquet(output_data + "songsplay/songsplay_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-elt/"

    process_song_data(spark, input_data, "output/")
    process_log_data(spark, input_data, "output/")


if __name__ == "__main__":
    main()

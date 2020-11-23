import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']        =config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']    =config['AWS']['AWS_SECRET_ACCESS_KEY']

print(os.environ['AWS_ACCESS_KEY_ID'] )
print(os.environ['AWS_SECRET_ACCESS_KEY'] )
#def create_spark_session():
#    spark = SparkSession \
#        .builder \
#        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
#        .getOrCreate()
#    print("spark session created")
#    return spark

def create_spark_session():
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
    .getOrCreate()
    print("spark session created")
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data +"song_data/*/*/*/*.json"
                             
    print(song_data)
    # read song data file
    print("reading in song data")
    df = spark.read.json(song_data)
        
    # extract columns to create songs table
    df.createOrReplaceTempView("songs_table_df")
    
    songs_table = spark.sql("""
                            SELECT song_id, title, artist_id,year, duration
                            FROM songs_table_df
                            ORDER by song_id
                            """) 
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_path = output_data + "songs_table.parquet"
    
    print("read to songs table to parquet format")
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(songs_table_path)

    
    # extract columns to create artists table
    df.createOrReplaceTempView("artist_table_df")
    artists_table = spark.sql( """
                                SELECT artist_id AS artist_id,
                                artist_name AS name,
                                artist_location AS location,
                                artist_latitude AS latitude,
                                artist_longitute AS longitude
                                FROM artist_table_df
                               """)
    
    # write artists table to parquet files
    artists_table_path = output_data + "artists_table.parquet"
    
    print("write to artist table")
    artists_table.write.mode("overwrite").parquet(artist_table_path)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"
    
    print("reading in log data")
    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df.page == 'NextSong')

    # extract columns for users table    
    df_log.createOrReplaceTempView("users_table_df")
    users_table = spark.sql("""
                              SELECT DISTINCT userId AS userid,
                              firstName AS first_name,
                              lastName AS last_name,
                              gender,
                              level
                              FROM users_table_df
                              ORDER BY last_name
                              """)
    
    print("writing to parquet format")
    # write users table to parquet files
    users_table_path = output_data + "users_table.parquet"
    
    users_table.write.mode("overwrite").parquet(users_table_path)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)),TimestampType())
    df_log = df_log.withColumn("timestamp", gettimestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(ts/1000.0).strfrmtime('%Y-%m-%d %H:%M:%S'))
    df_log = df_log.withColumn("datetime",get_datetime("ts"))
    
    # extract columns to create time table
    df_log.createOrReplaceTempView("time_table_df")
    
    time_table = spark.sql("""SELECT DISTINCT 
                             datetime as start_time,
                             hour(timestamp) as hour,
                             day(timestamp) as day,
                             weekofyear(timestamp) as week,
                             month(timestamp) as month,
                             year(timestamp) as year,
                             dayofweek(timestamp) as weekday
                             FROM time_table_df
                             ORDER BY start_time
    
                           """)
    
    # write time table to parquet files partitioned by year and month
    time_table_path = output_data + "time_table.parquet"
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(time_table_path)

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    
    #join log and song df together
    df_log_song_df_joined = df_log_filtered.join(df_song, (df_log_filtered.artist == df_song.artist_name) & (df_log_filtered.song == df_song.title))
    
    # extract columns from joined song and log datasets to create songplays table 
    
    df_log_song_df_joined.createOrReplaceTempView("songplays_table_df")
    
    songplays_table = spark.sql("""
        SELECT  songplay_id AS songplay_id,
                timestamp   AS start_time,
                userId      AS user_id,
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent
        FROM songplays_table_DF
        ORDER BY (user_id, session_id)
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data + "songplays_table.parquet"
    
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(songplays_table_path)
    
    return users_table, time_table, songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-lake/output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

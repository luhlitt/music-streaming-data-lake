import configparser
from datetime import datetime
import os
# import findspark
# findspark.init()

# import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Create spark session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    Loads song data from S3,
    extracts columns to create the song and aritst tables, and 
    writes data to parquet files to be loaded into S3. 
    
    """
    # get filepath to song data file
    song_data = input_data + "song-data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").where(df.artist_id != "")
    songs_table.show() # print statement for debugging
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songs'), partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").where(df.artist_id != "")
    artists_table.show() # print statement for debugging

    # write artists table to parquet files
    artists_table = artists_table.write.mode("overwrite").parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """ 
    Loads log data from S3,
    extracts and pre-process columns to create the user, time and songplay tables, and 
    writes data to parquet files to be loaded into S3. 
    
    """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").where(df.userId != "").dropDuplicates()
    users_table.show() # print statement for debugging
    
    # write users table to parquet files
    users_table = users_table.write.mode("overwrite").parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                    hour('timestamp').alias('hour'),
                    dayofmonth('timestamp').alias('day'),
                    weekofyear('timestamp').alias('week'),
                    month('timestamp').alias('month'),
                    year('timestamp').alias('year'),
                    date_format('timestamp','E').alias('weekday'))
    time_table.show() # print statement for debugging

    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet(os.path.join(output_data, 'time'), partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df =spark.read.json(input_data + "song-data/A/A/A/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name)
                        & (df.length == song_df.duration), "left_outer") \
                            .select(
                                df.timestamp,
                                col("userId").alias('user_id'),
                                df.level,
                                song_df.song_id,
                                song_df.artist_id,
                                col("sessionId").alias("session_id"),
                                df.location,
                                col("useragent").alias("user_agent"),
                                year('timestamp').alias('year'),
                                month('timestamp').alias('month'))
    songplays_table.show() # print statement for debugging

    # write songplays table to parquet files partipython etl.pytioned by year and month
    songplays_table.write.mode("overwrite").parquet(os.path.join(output_data, 'songplays'), partitionBy=['year', 'month'])

def main():
    print("Creating spark session..")
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://myfirstsparkjob/"
    print(" spark session created")
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

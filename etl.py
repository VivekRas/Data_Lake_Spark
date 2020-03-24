from pyspark.sql import SparkSession
import os
import configparser
from pyspark.sql import Window
from pyspark.sql import Row, Column, functions as F
from pyspark.sql import types as T
from datetime import datetime
from pyspark.sql.functions import month, year, hour, dayofweek, dayofyear, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    This function reads song_data from S3 (public folder), makes songs and artists tables and uploads them back on S3
    
    Parameters: 
    spark : SparkSession
    input_data : public S3 path where input data is scored
    output_data : Our S3 path where output data is scored
  
    Returns: 
    song and artist tables are saved as parquet files on a personal folder on S3
    """
    # get filepath to song data file
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).where(df["song_id"].isNotNull())
    
    # write songs table to parquet files partitioned by year and artist
    song_data_out = f'{output_data}/songs_table/songs_table.parquet'
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(song_data_out)

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).where(df["artist_id"].isNotNull())
    
    # write artists table to parquet files
    artists_data_out = f'{output_data}/artists_table/artists_table.parquet'
    artists_table.write.mode('overwrite').parquet(artists_data_out)

def process_log_data(spark, input_data, output_data):
    """
    This function reads song_data and log_data from S3 (public folder), 
    makes user, artists and songplay tables and uploads them back on S3
    
    Parameters: 
    spark : SparkSession
    input_data : public S3 path where input data is scored
    output_data : Our S3 path where output data is scored
  
    Returns: 
    user, artists, songplay tables are saved as parquet files on a personal folder on S3
    """
    # get filepath to log data file
    log_data = f'{input_data}/log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page =='NextSong')

    # extract columns for users table    
    user_table = df.select(["userId", "firstname", "lastname", "gender", "level"]).where(df["userId"].isNotNull())
    
    # write users table to parquet files
    user_data_out = f'{output_data}/user_table/user_table.parquet'
    user_table.write.mode('overwrite').parquet(user_data_out)

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(['timestamp']).dropDuplicates()
    time_table = time_table.withColumn("hour", hour(time_table["timestamp"]))
    time_table = time_table.withColumn("day", dayofyear(time_table["timestamp"]))
    time_table = time_table.withColumn("week", weekofyear(time_table["timestamp"]))
    time_table = time_table.withColumn("month", month(time_table["timestamp"]))
    time_table = time_table.withColumn("year", year(time_table["timestamp"]))
    time_table = time_table.withColumn("weekday", dayofweek(time_table["timestamp"]))

    
    # write time table to parquet files partitioned by year and month
    time_data_out = f'{output_data}/time_table/time_table.parquet'
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(time_data_out)

    # read in song data to use for songplays table
    song_data = f'{input_data}/song_data/*/*/*/*.json'
    sdf = spark.read.json(song_data)
    sdf.createOrReplaceTempView("song_df_table")
    
    # Adding month and year column to log data read and preparing log data table
    df = df.withColumn("month", month(df["timestamp"]))
    df = df.withColumn("year", year(df["timestamp"]))
    df.createOrReplaceTempView("log_df_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT ldf.timestamp as start_time,
        ldf.userid as user_id,
        ldf.level,
        sdf.song_id,
        sdf.artist_id,
        ldf.sessionid as session_id,
        ldf.location,
        ldf.useragent as user_agent,
        ldf.month,
        ldf.year
    FROM log_df_table ldf
    JOIN song_df_table sdf
    ON (ldf.song = sdf.title) AND (ldf.artist = sdf.artist_name) AND (ldf.length = sdf.duration)
    WHERE ldf.page = 'NextSong' and ldf.userid is not null
    """)
    
    # adding the songplay_id column
    window = Window.orderBy(F.col('start_time'))
    songplays_table = songplays_table.withColumn('songplay_id', F.row_number().over(window))
    songplays_table.select('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent', 'month', 'year').show()

    # write songplays table to parquet files partitioned by year and month
    songplays_data_out = f'{output_data}/songplays_table/songplays_table.parquet'
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(songplays_data_out)

def main():
    """
    This function is the main fns, we use it to call all the other fns one after the other
    and finally close the connection
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://vivek1bucket"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__": # the etl.py program, starts here and gets redirected to main fns
    main()

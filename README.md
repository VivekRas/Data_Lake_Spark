# Data lake with Spark
## Tools - Spark SQL, Spark DataFrames, Python and AWS

ETL pipeline that extracts data from S3, processes it using Spark, and loads it back on S3 as a set of dimensional tables into partitioned parquet files

## Purpose of this database 
As their database grows Sparkify has migrated on the cloud. 
Their data resides in S3 in a directory of JSON logs on "user activity on the app", as well as a directory with JSON metadata on the "songs in their app".
We need to implement a data lake to allow allow their analytics team to continue finding insights in what songs their users are listening to

Their purpose is two fold 
1. They need the flexibility of a data lake and at the same time an ability to run simple queries, that take the minimum amount of time while giving them wholesome data
2. Need a data pipeline that can be run every day to update the data lake.

## Project Template

1. `dl.cfg` has AWS credentials
2. `etl.py` reads data from S3, processes that data using Spark, and writes them back to S3. 

## Database schema design

Using the song and log datasets, we have created a star schema optimized for queries on song play analysis. This includes the following tables.

**Fact Table**
1. songplays - records in log data associated with song plays i.e. records with page NextSong

**Dimension Tables**
2. users - users in the app
3. songs - songs in music database
4. artists - artists in music database
5. time - timestamps of records in songplays broken down into specific units

_Data is in a structured form (extracted from json format to a more readable table format)_
_We have done data quilty checks - Cleaned it up, removed duplicated, removed null values_

## ELT pipeline (Steps to run the program)

0. Make sure that the data is stored in the filepath='s3://udacity-dend/song_data' for songs data and filepath='s3://udacity-dend/log_data' for logs data
1. Create an output data directory on S3, create subdirectories for each table
2. save aws credentials on dl.cfg file  
2. cmd > "python etl.py" > enter

(The five sub directories on Amazon S3 get updated with partitioned parquet files in their respective table directories)

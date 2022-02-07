import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads song data from the s3 bucket, transforms it as needed,
    and writes it to a song table and an artist table in respective
    parquet files that are then loaded into the defined s3 bucket.
    
    spark: the spark session created during create_spark_session
    
    input_data: path for our data files being read
    
    output_data: path for our parquet files being written
    """
    
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    
    df = spark.read.json(song_data)

 
    songs_table = df.select('song_id','title','artist_id','year','duration').dropDuplicates()
    

    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+'songs/')

    
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude')
    
    
    artists_table.write.mode('overwrite').parquet(output_data+'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Reads log data from the s3 bucket, transforms it as needed,
    and writes it to a user table and time table in respective
    parquet files that are then loaded into the defined s3 bucket.
    Then re-reads the song data to join with the log data based on
    song information. This data is placed into the songplay data and
    also written to a parquet file to be loaded in the defined s3
    bucket.
    
    spark: the spark session created during create_spark_session
    
    input_data: path for our data files being read
    
    output_data: path for our parquet files being written
    """
    
    log_data = input_data + 'log_data/*/*/*.json'

    
    df = spark.read.json(log_data)
    
    
    df = df.filter(df.page == 'NextSong')
    
    users_table = df.select('userId','firstName','lastName','gender','level').dropDuplicates()
    
    users_table.write.parquet(output_data + 'users/')

    get_timestamp = udf(lambda x: x/1000,TimestampType())
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    get_datetime = udf(lambda x: from_unixtime(x),TimestampType())
    df = df.withColumn('datetime',get_datetime(df.ts))
    
    time_table = df.select(col('datetime').alias('start_time')) \
                    .withColumn('hour', hour('datetime')) \
                    .withColumn('day', dayofmonth('datetime')) \
                    .withColumn('week',weekofyear('datetime')) \
                    .withColumn('month',month('datetime')) \
                    .withColumn('year', year('datetime')) \
                    .withColumn('weekday',dayofweek('datetime')) \
                    .dropDuplicate()
    
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'time/')

    song_df = spark.read.json(input_data + 'song_data')

    songplays_table = song_df.join(song_df, (df.song == song_df.title) & \
                                   (df.artist == song_df.artist_name) & \
                                   (df.length == song_df.duration) \
                                   , 'left_outer') \
                                    .select(col('datetime').alias('start_time') \
                                            ,col('userId').alias('user_id') \
                                            ,df.level \
                                            ,song_df.song_id \
                                            ,song_df.artist_id \
                                            ,col('sessionId').alias('session_id') \
                                            ,df.location \
                                            ,col('useragent').alias('user_agent') \
                                            ,year('datetime').alias('year') \
                                            ,month('datetime').alias('month')) \
                                    .withColumn('songplay_id',monotonically_increasing_id())

    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data, 'songplays')


def main():
    """
    First creates the spark session, then defines our input
    and output paths. With these variables the song data is processed
    followed by the log data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://pkudacitybucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()

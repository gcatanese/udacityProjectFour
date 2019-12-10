import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.types import TimestampType

from pyspark.sql.functions import *

config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID']='1'
os.environ['AWS_SECRET_ACCESS_KEY']='2'


def create_spark_session():
    """
    Create Spark session
    :return: Spark session
    """
    # spark = SparkSession \
    #     .builder \
    #     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
    #     .getOrCreate()
    spark = SparkSession \
        .builder \
        .appName("Udacity project 4") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process SONG data

    :param spark: Spark session
    :param input_data: location of the data
    :param output_data: location of the generated output (Parquet files)
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)
    print(df)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates(['song_id'])
    print(songs_table.show(2))

    #songs_table.createOrReplaceTempView("SONGS_TABLE")

    # # write songs table to parquet files partitioned by year and artist
    #songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table')

    # # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates(['artist_id'])
    print(artists_table.show(2))

    #
    # # write artists table to parquet files
    #artists_table.write.mode('overwrite').parquet(output_data+'artists_table')


def process_log_data(spark, input_data, output_data):
    """
    Process LOG data

    :param spark: Spark session
    :param input_data: location of the data
    :param output_data: location of the generated output (Parquet files)
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(col('userId').alias('user_id'), col('firstName').alias('first_name'),col('lastName').alias('last_name'), 'gender', 'level').dropDuplicates(['user_id'])
    print(users_table.show(2))

    # write users table to parquet files
    #users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn("timestamp", get_timestamp("ts"))
    print(df.show(2))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:datetime.fromtimestamp(x / 1000.0), DateType())
    df = df.withColumn("date", (get_datetime("ts")))
    print(df.show(2))

    # extract columns to create time table
    time_table = df.select(col("ts").alias("start_time"), hour(df.date).alias('hour'), dayofmonth(df.date).alias('day'), weekofyear(df.date).alias('week'),
                           month(df.date).alias('month'), year(df.date).alias('year'), dayofweek(df.date).alias('weekday'))
    print(time_table.show(2))

    # write time table to parquet files partitioned by year and month
    #time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    songplays= song_df.join(df, (df.artist == song_df.artist_name) & (df.song == song_df.title))

    songplays = songplays.withColumn("id", monotonically_increasing_id())
    print(songplays.show(2))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = songplays.select('id', col("ts").alias("start_time"), col("userId").alias("user_id"), "level",
                                      'song_id', 'artist_id', col("sessionId").alias("session_id"),
                                       "location", col("userAgent").alias("user_agent"))
    print(songplays_table.show(2))

    # write songplays table to parquet files partitioned by year and month
    #songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table')


def main():
    """
    Main: runs the pipeline
    """
    spark = create_spark_session()
    input_data = "s3a://beppe-dend/"
    output_data = "s3a://beppe-udacity/"
    input_data = "data/"
    output_data = "data/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()

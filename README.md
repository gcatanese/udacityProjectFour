# Project: Data Lake

## Intro

The goal of the project is to create a Data Lake using Spark and AWS.
The song and log data is loaded from Amazon S3, processed and the output stored back on S3.

The README presents the following:
1. Solution
2. How to execute the ETL
3. Data model (Facts and Dimension tables)

## Solution

The ETL processes 2 sets of file: SONG and LOG data. The data is available on an S3 bucket in the form of JSON files, 
organised in a folder structure. 
The Spark Dataframe is used to load the files in a tabular format, mapping JSON attributes to the desired column names and dealing with possible duplicates.

User Defined Functions (UDF) are used to process specific columns, for example to convert a BigInt value to the corresponding Timestamp.

Two different Dataframes are combined (join) to create the Facts table as this needs data found in different datasets.

The facts and dimensions tables are saved as Parquet files (partitioned when needed) and stored back on S3, ready for the
Sparkify team to load and query.


## Execute the ETL

### Pre-requisites

* Python 3 with pyspark
* S3 bucket with the SONG and LOG data

1. Config dl.cfg accordingly (AWS keys)
2. Execute 'etl.py'

## Data Model

### Fact & Dimensions tables

The schema is designed around the Star Schema principle with one fact table and several dimensions:
1. SONG_PLAYS (fact)
2. USERS
3. SONGS
4. ARTISTS
5. TIME

  

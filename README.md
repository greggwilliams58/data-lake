# Udacity Sparkify Data Lake Project with Apache Spark

The purpose of this project is to build an ETL pipeline that will be able to extract song and log data from an S3 bucket, process the data using Spark and load the data back into s3 as a set of dimensional tables in spark parquet files. This helps analysts to continue finding insights on what their users are listening to.

# Database Schema Design
There is one fact table - songplays There are four dimensional tables - users, songs, artists and time. This follows the star schema principle which will contain clean data that is suitable for OLAP(Online Analytical Processing) operations which will be what the analysts will use to find the insights they are looking for.

# ETL Pipeline
The extracted data will be transformed to fit the data model of the target destination tables. For instance the source data for timestamp is in unix format and that will need to be converted to timestamp from which the year, month, day, hour values etc can be extracted which will fit in the relevant target time and songplays table columns. The script will also need to cater for duplicates, ensuring that they aren't part of the final data that is loaded in the tables.

# Datasets used
The datasets used are retrieved from the udacity s3 bucket and hold files in a JSON format. There are two datasets: log_data and song_data. The song_data dataset is a subset of the the Million Song Dataset while the log_data contains generated log files based on the songs in song_data.

# Project Files
## etl.py
This script once executed retrieves the song and log data in the s3 bucket, transforms the data into fact and dimensional tables then loads the table data back into s3 as parquet files.

## dl.cfg
Will contain your AWS keys, but to protect mine, this file on github has dummy values

# Getting Started
## Requirements
- Python 2.7 or higher
- AWS Account
- Put your AWS access and secret key in the config file in the following format (no quotation marks are needed)

> [AWS]
> AWS_ACCESS_KEY_ID = your aws key
> AWS_SECRET_ACCESS_KEY = your aws secret key

## To execute the code
start code with the following call:

spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 etl.py
    

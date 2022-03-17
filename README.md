# Schema for Song Play Analysis 

Sparkify requires an ETL pipeline to handle the activity of a large and growing userbase. This repo has python code and sql queries for a data pipeline in AWS (S3, EMR and Spark) that creates analytical database for song play analysis. Analysis could include summary statistics for song plays by user type, time, artist, and location, among many other possibilities.

## Setup

### Install

Start by creating a key-pair on your AWS account. Then clone the git repo:
```
git clone git@github.com:twedl/sparkify-datalake.git
cd sparkify-datalake
```
Then modify the `create-emr-cluster.sh` to your liking (key information, subnet group and bootstrap script) and run:
```
. create-emr-cluster.sh
```
Make sure you change the output S3 bucket in `etl.py` to one in which you have permissions to write. Wait until the cluster is ready and waiting, then copy your files onto the EMR master node by:
```
scp -i <YOUR KEY>.pem dl.cfg etl.py hadoop@<YOUR EMR MASTER DNS>:/home/hadoop/
```
Then ssh into the master node:
```
ssh -i <YOUR KEY>.pem hadoop@<YOUR EMR MASTER DNS>
```
Make sure the correct python packages are installed and then submit the pyspark script:
```
sudo pip install configparser pyspark
sudo spark-submit --master yarn etl.py
```

Done!


### Files

* `create-emr-cluster.sh`: command to create the EMR cluster
* `bootstrap-emr-sh`: file that `create-emr-cluster.sh` uses to install important packages; must be uploaded to S3 bucket
* `dl.cfg`: (Config file with AWS access and secret keys; not included)
* `etl.py`: script to ingest all the data in the S3 buckets and insert into the staging and analytical tables

### Run

To create the database and insert the data into tables, run:
```
sudo spark-submit --master yarn etl.py
```

After running this, the S3 bucket with path `output_data` in `etl.py` should exist with parquet files `songplays`, `users`, `songs`, `artists`, and `time`. 

## Schema

The database uses a star schema optimized for song play analytical queries. It includes the following tables:

The fact table is `songplays` (populated from files located in the S3 buckets `s3://udacity-dend/log_data`, with columns `song_id` from `songs` table and `artist_id` from `artist` table)
- `songplays` (fact table); columns: 
  - `songplay_id`,`start_time`,`user_id`,`level`,`song_id`,`artist_id`,`session_id`,`location`,`user_agent`

The dimensions tables are populated with data from the song information in the S3 buckets `s3://udacity-dend/song_data`. They include the following tables:

- `users`: users in the app, with columns: 
  - `user_id`,`first_name`,`last_name`,`gender`,`level`
- `songs`: songs in the music database, with columns:
  - `song_id`,`title`,`artist_id`,`year`,`duration`
- `artists`: artists in the music database, with columns:
  - `artist_id`,`name`,`loction`,`latitude`,`longitude`
- `time`: timestamps of records in `songplays` broken down into specific units
  - `start_time`,`hour`,`day`,`week`,`month`,`year`,`weekday`

This star schema was chosen to reduce redundency in the database; this reduces storage required as well as reducing the chance that errors are introduced because song and artist information can be updated in one table only.

Using the star schema may increase query time if every query requires joining the dimension tables to the fact tables to produce results. In this case, future schemas may include commonly required dimensions into the fact table itself. However, this increases the likelihood of errors introduced by updating dimensions in the dimension table but not the fact table, or vice versa.

## ETL Pipeline

The pipeline: 

1. Create EMR cluster (via AWS CLI command in bash script) 
2. Read all song files from S3
3. Use song files to create song and artists datasets and write them to partitioned parquet files in output S3 bucket
4. Read all log files from S3
5. Use log files to user and time tables and write them to S3 bucket
6. Filter log for songplay events (page == "NextSong")
6. Use song and artists tables from step 3 to find song id and artist id for each songplay in the log
7. Write songplay table to output S3 bucket


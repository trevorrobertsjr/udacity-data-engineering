# Project: Data Warehouse

## Introduction
Sparkify is a music startup that wishes to analyze musict listening behavior of its users for planning future product features and marketing promotions. 

## Purpose of the Database
My task was to use Sparkify's user events logs and music database to create a data warehouse on [Amazon Redshift](https://aws.amazon.com/redshift/faqs/). This data warehouse will be used by Sparkify's business analysts to collect the data required by their product management and marketing teams.

## Database Schema Design and ETL Pipeline
The code in this repo performs extract, load, and transform (ELT) operations in Redshift with Sparkify's source data stored in [Amazon S3](https://aws.amazon.com/s3/faqs/) buckets. The ELT pipeline consists of Python scripts that use the [psycopg2](https://www.psycopg.org/docs/) library for the following actions:
* execute Redshift copy commands of the source data (user events and the song library) to staging tables (staging_events and staging_songs, respectively).
* implement a star schema by creating a facts table (songplays) that contains the user play event timestamps and the sort keys for four dimension tables (users, songs, artists, time).
* run required SQL queries 

Here are brief summaries of the data stored in the dimensions tables:
* users - Sparkify's users' full names, their genders, and their membership levels
* songs - songs' titles, artist id's, year released, and duration
* artists - artists' names, locations, and coordinates
* time - timestamps from users' listening  events with granular details including the hour, day, week, month, and year  

![Sparkify ETL Diagram shows source in Amazon S3, staging tables that need to be created and the star schema tables that will be generated for reports](img/project2-sparkify-s3-to-redshift-etl.png)

## How to Use this Repo
### Prerequisites
1. Create an IAM role with approprirate permissions for Redshift to assume
1. Deploy a Redshift Cluster or Redshift Serverless with the IAM role attached
1. Configure a security group to allow your IP address to access
1. Create a configuration file called **dwh.cfg** in the root of the repo that contains the following information:
```conf
[CLUSTER]
HOST=# Your Redshift Cluster/Serverless Endpoint
DB_NAME=# Your Redshift Database
DB_USER=# Your Redshift Username
DB_PASSWORD=# Your Redshift Password
DB_PORT=# Your Redshift Database Port to Connect to. Remember to configure your security group!

[IAM_ROLE]
ARN=# The IAM Role you assigned to Redshift.

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```
### Build the Environment
Executing the following command that accomplishes the following:
1. Run the **create_tables.py** script that drops any previous iterations of the staging and destination tables, and it recreates them.
1. Run the **etl.py** script that populates the staging tables first and then uses these tables to populate the fact and dimension tables.
#### Run the Code
```bash
$ python create_tables.py && python etl.py
```

### Environment Cleanup
Make sure to delete your cluster if you are completely done with the evaluation. If you will retain the cluster, remove the Sparkify data and tables by using the **cleanup.py** script:

```bash
$ python cleanup.py
```
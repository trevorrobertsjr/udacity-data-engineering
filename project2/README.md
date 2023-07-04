# Project: Data Warehouse

## Prerequisites
1. Create an IAM role with approprirate permissions for Redshift to assume
1. Deploy a Redshift Cluster or Redshift Serverless with the IAM role attached
1. Configure a security group to allow your IP address to access
1. Create a configuration file called **dwh.cfg** in the root of this folder that contains the following information:
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

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

![Sparkify ETL Diagram shows source in Amazon S3, staging tables that need to be created and the star schema tables that will be generated for reports](img/project2-sparkify-s3-to-redshift-etl.png)


## Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.



## Song Dataset
The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.



## Document Process
Do the following steps in your README.md file. Here's a guide on Markdown Syntax.

Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
State and justify your database schema design and ETL pipeline.

The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository. Comments are used effectively and each function has a docstring.

[Optional] Provide example queries and results for song play analysis. We do not provide you any of these. You, as part of the Data Engineering team were tasked to build this ETL. Thorough study has gone into the star schema, tables, and columns required. The ETL will be effective and provide the data and in the format required. However, as an exercise, it seems almost silly to NOT show SOME examples of potential queries that could be ran by the users. PLEASE use your imagination here. For example, what is the most played song? When is the highest usage time of day by hour for songs? It would not take much to imagine what types of questions that corporate users of the system would find interesting. Including those queries and the answers makes your project far more compelling when using it as an example of your work to people / companies that would be interested. You could simply have a section of sql_queries.py that is executed after the load is done that prints a question and then the answer.


## Build the Environment

Executing the following command that accomplishes the following:
1. Run the **create_tables.py** script that drops any previous iterations of the staging and destination tables, and it recreates them.
1. Run the **etl.py** script that populates the staging tables first and then uses these tables to build the 
### Run the Code

```bash
$ python create_tables.py && python etl.py
```

## Environment Cleanup

Make sure to delete your cluster if you are completely done with the evaluation. If you will retain the cluster execute the following command

```bash
$ python cleanup.py
```
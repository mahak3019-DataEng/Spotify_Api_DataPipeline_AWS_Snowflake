# SPOTIFY API Data Pipeline with AWS Lambda and Snowflake

### Connection with Spotify Web API
Link for connection: https://developer.spotify.com/documentation/web-api/tutorials/implicit-flow

Link: https://developer.spotify.com/documentation/web-api/tutorials/getting-started

Youtube Tutorials link: https://www.youtube.com/watch?v=WAmEZBEeNmg&t=1296s

### Project Description
This project involves creating a data pipeline using AWS Lambda functions and Snowflake. The pipeline performs the following tasks:

### Data Retrieval:

An AWS Lambda function retrieves data from the Spotify API and stores the JSON file in an S3 bucket.
Environment variables are used to maintain the privacy of API keys and other sensitive information.
Data Transformation:

### Data Transformation
A second AWS Lambda function extracts data from the S3 bucket, transforms it, and then stores the transformed data back in the S3 bucket.


### Scheduling and Triggers:

The first Lambda function is scheduled using AWS CloudWatch to run every hour.
The second Lambda function is triggered by changes in the S3 bucket (i.e., when new transformed data is stored).

### Data Ingestion to Snowflake:

Snowpipe is set up to automatically ingest the transformed data from the S3 bucket into a Snowflake data warehouse for analysis.

### Data Catalog and Analysis:

AWS Glue Crawler is used to create a data catalog for the transformed data.
AWS Athena is used to perform SQL queries on the cataloged data.
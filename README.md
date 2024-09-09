# SPOTIFY API Data Pipeline with AWS S3, AWS Lambda and Snowflake

### Connection with Spotify Web API
Link for connection: https://developer.spotify.com/documentation/web-api/tutorials/implicit-flow

Link: https://developer.spotify.com/documentation/web-api/tutorials/getting-started

Youtube Tutorials link: https://www.youtube.com/watch?v=WAmEZBEeNmg&t=1296s

### Project Description
This project consists of two AWS Lambda functions that interact with AWS S3 and Snowflake. The project also integrates with AWS CloudWatch and Snowpipe for automated data processing.
The pipeline performs the following tasks:

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

## Project 2: SPOTIFY API Data Pipeline with AWS S3, Apache Airflow and Snowflake

## Overview

This project involves creating a data pipeline that extracts Spotify playlist data using the Spotify Web API, processes the data, and stores it in AWS S3 using Apache Airflow. The pipeline consists of several tasks, including data extraction, transformation, and loading (ETL), all orchestrated using Apache Airflow.

## Project Components

1. **Spotify API Data Extraction**
   - Uses the Spotify Web API to retrieve playlist data.
   - Authentication is handled via OAuth 2.0 using client credentials.

2. **Data Upload to AWS S3**
   - Raw JSON data from Spotify is uploaded to an S3 bucket for further processing.

3. **Data Processing**
   - The raw JSON data is read from the S3 bucket.
   - The data is transformed into a structured format (CSV) suitable for analysis.

4. **Data Upload to AWS S3**
   - The processed CSV data is uploaded back to the S3 bucket.

5. **File Management**
   - Moves processed files from a "to_process" folder to a "processed" folder within the S3 bucket.

## Prerequisites

- Apache Airflow
### for Airflow setup
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml'

mkdir -p ./dags ./logs ./plugins ./config

docker-compose up airflow-init
docker compose up -d
- AWS S3
- Python 3.x
- Spotify Developer Account

## Setup

1. **Apache Airflow Setup**
   - Ensure Apache Airflow is installed and configured.
   - Set up Airflow connections for AWS (using `aws_default`).

2. **Spotify API Credentials**
   - Obtain Spotify API credentials (Client ID and Client Secret).
   - Store these credentials in Airflow Variables (`client_id` and `client_secret`).

3. **AWS S3 Setup**
   - Create an S3 bucket (`spotify-api-project2-mahak`) with the following folders:
     - `raw_data/to_processed/`
     - `transformed_data/track_songs_data/`

## DAG Description

The DAG `spotify_data_api` consists of the following tasks:

1. **`extract_data_from_spotify`**
   - Extracts data from a specified Spotify playlist.
   - Pushes the raw JSON data and filename to XCom.

2. **`upload_raw_data_to_s3`**
   - Uploads the raw JSON data to an S3 bucket.

3. **`read_data_from_s3`**
   - Reads the raw JSON files from the S3 bucket.
   - Pushes the data and file keys to XCom.

4. **`process_data_from_s3`**
   - Transforms the raw JSON data into a structured CSV format.
   - Pushes the transformed data and filename to XCom.

5. **`upload_transformed_data_back_to_s3`**
   - Uploads the transformed CSV data back to the S3 bucket.

6. **`copy_toprocessed_to_processed`**
   - Moves processed JSON files from the "to_processed" folder to the "processed" folder in the S3 bucket.

## Running the DAG

- The DAG is scheduled to run daily.
- To trigger the DAG, use the Airflow web interface or CLI.

## Error Handling

- Logs are available in the Airflow web interface for debugging.
- Ensure proper error handling in each task to manage potential issues.
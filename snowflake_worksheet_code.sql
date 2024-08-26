Create or replace database SPOTIFY_DB;


CREATE OR REPLACE SCHEMA SPOTIFY_DB.spotify_schema;

show databases;

CREATE OR REPLACE STORAGE INTEGRATION spotify_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED= TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::533267281673:role/spotify_snowflake_aws_integration_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-api-project2-mahak/transformed_data/track_songs_data/','s3://spotify-api-project2-mahak/raw_data/processed/')
COMMENT = 'Spotify Snowflake integration with AWS';

desc integration spotify_integration;



create or replace file format spotify_db.spotify_schema.processed_file_format
type = 'JSON';

desc file format spotify_db.spotify_schema.processed_file_format;


create or replace stage spotify_stage_processed
storage_integration = spotify_integration
file_format = spotify_db.spotify_schema.processed_file_format
url = 's3://spotify-api-project2-mahak/raw_data/processed/';

list @spotify_db.spotify_schema.spotify_stage_processed;





create or replace file format spotify_db.spotify_schema.transformed_file_format
type = 'CSV'
field_delimiter = ','
skip_header = 1
field_optionally_enclosed_by = '"'
null_if = ('NULL','null')
empty_field_as_null = TRUE; 

create or replace stage spotify_stage_transformed
storage_integration = spotify_integration
file_format = spotify_db.spotify_schema.transformed_file_format
url = 's3://spotify-api-project2-mahak/transformed_data/track_songs_data/';

list @spotify_db.spotify_schema.spotify_stage_transformed;


-- creating table which will store transformed value
CREATE OR REPLACE TABLE spotify_db.spotify_schema.transformed_data(
track_id VARCHAR(255),
track_name STRING,
track_artists STRING,
track_album_id VARCHAR(255),
track_album_name STRING,
track_album_release_date DATE,
track_album_total_tracks INTEGER,
track_album_artists STRING,
track_album_url VARCHAR(255)
);

copy into spotify_db.spotify_schema.transformed_data
FROM @spotify_db.spotify_schema.spotify_stage_transformed
FILE_FORMAT = (FORMAT_NAME = 'spotify_db.spotify_schema.transformed_file_format')
ON_ERROR = 'CONTINUE';


SELECT * FROM spotify_db.spotify_schema.transformed_data;




show pipes;

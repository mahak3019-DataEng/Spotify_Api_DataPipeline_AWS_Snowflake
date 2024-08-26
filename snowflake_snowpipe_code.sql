USE DATABASE SPOTIFY_DB;

USE SCHEMA SPOTIFY_DB.SPOTIFY_SCHEMA;


CREATE OR REPLACE PIPE spotify_db.spotify_schema.aws_snowflake_snowpipe
auto_ingest = True
AS 
COPY INTO spotify_db.spotify_schema.transformed_data // name of the table you created
FROM @spotify_db.spotify_schema.spotify_stage_transformed; // name of the stage you created

DESC PIPE spotify_db.spotify_schema.aws_snowflake_snowpipe;

select count(*) from spotify_db.spotify_schema.transformed_data;
select * from spotify_db.spotify_schema.transformed_data;


SHOW PIPES IN DATABASE spotify_db;

SHOW PIPES IN SCHEMA spotify_db.spotify_schema;

show pipes like 'aws%';
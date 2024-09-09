from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3
import requests
from requests import post, get
import base64
from airflow.models import Variable
import json
import pandas as pd
from io import StringIO
import logging

default_args = {
    'owner': 'mahak_spotify',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# function to retrieve data from Spotify Web API.
def extract_data_from_spotify(**kwargs):
    client_id = Variable.get('client_id')
    client_secret = Variable.get('client_secret')
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "client_credentials"
        }
    response = requests.post(url, headers=headers, data=data)
    response_data = response.json()
    access_token = response_data['access_token']
    def get_auth_header(token):
        return {"Authorization": "Bearer " + token}

    # Creating the function to get the top 100 songs on Spotify.
    playlist_link = "https://open.spotify.com/playlist/4JZFUSM0jb3RauYuRPIUp8"
    playlist_id = playlist_link.split('/')[-1]  
    
    # get the result in the form of json file.
    def get_playlist_tracks(access_token, playlist_id):
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        headers = get_auth_header(access_token)
        result = get(url, headers=headers)
        json_result = json.loads(result.content)
        return json_result
        # return json_result
    file_name = "spotify_raw_" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".json"

    # pushing data to xcom, so that other function can use the result.
    kwargs['ti'].xcom_push(key='spotify_json_data',value=json.dumps(get_playlist_tracks(access_token, playlist_id)))
    kwargs['ti'].xcom_push(key='spotify_raw_file_name', value = file_name)
    

# uploading json raw file to S3 bucket using xcom_pull function.
def upload_raw_data_to_s3(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids = 'extract_data_from_spotify', key='spotify_json_data')
    file_name = ti.xcom_pull(task_ids = 'extract_data_from_spotify', key='spotify_raw_file_name')

    if json_data is None:
        raise ValueError("No JSON data retrieved from XCom")
    if file_name is None:
        raise ValueError("No filename retrieved from XCom")
    
    s3hook = S3Hook(aws_conn_id = 'aws_default')
    bucket_name = "spotify-api-project2-mahak"
    object_key = "raw_data/to_processed/" + file_name

    # as my json data is already in json format we will 
    # upload in the string format using s3hook.load_string()

    s3hook.load_string(
        string_data= json_data,
        bucket_name = bucket_name,
        key = object_key
    )

#  get the data from S3 bucket to process it.
def read_data_from_s3(**kwargs):
    s3hook = S3Hook(aws_conn_id = 'aws_default')
    bucket_name = "spotify-api-project2-mahak"
    prefix = "raw_data/to_processed/"
    
    keys = s3hook.list_keys(bucket_name = bucket_name, prefix=prefix)
    # print(keys)
    spotify_data_file = []
    spotify_key_file = []
    for key in keys:
        if key.split(".")[-1] == 'json':
            json_data = s3hook.read_key(key, bucket_name)
            spotify_data_file.append(json.loads(json_data))
            spotify_key_file.append(key)
    kwargs['ti'].xcom_push(key='spotify_read_data',value = spotify_data_file)
    kwargs['ti'].xcom_push(key='spotify_key_file', value =spotify_key_file)


# transform the data into structured format. Make data ready for analysis.
def process_data_from_s3(**kwargs):
    spotify_data_file = kwargs['ti'].xcom_pull(task_ids = 'read_data_from_s3', key='spotify_read_data')
    
    transform_data_file = []
    for tracks_json in spotify_data_file:
        track_name = []
        for item in tracks_json['items']:
            track_name.append(item['track']['name'])
        
        track_id = []
        for item in tracks_json['items']:
            track_id.append(item['track']['id'])
        
        track_artists_list = []
        for item in tracks_json['items']:
            track_artists_list.append(item['track']['artists'])
        track_artists = []
        for artist in track_artists_list:
            track_artist_name = []
            for artist_name in artist:
                track_artist_name.append(artist_name['name'])
            track_artists.append(track_artist_name)
        
        #Converting this list into String format of this list
        track_artists_converted =[]
        for track in track_artists:
            track = '; '.join(track)
            track_artists_converted.append(track)
            
        track_album_id = []
        for item in tracks_json['items']:
            track_album_id.append(item['track']['album']['id'])
            
        track_album_name = []
        for item in tracks_json['items']:
            track_album_name.append(item['track']['album']['name'])
            
        track_album_release_date = []
        for item in tracks_json['items']:
            track_album_release_date.append(item['track']['album']['release_date'])
            
        track_album_total_tracks = []
        for item in tracks_json['items']:
            track_album_total_tracks.append(item['track']['album']['total_tracks'])
            
        track_album_url = []
        for item in tracks_json['items']:
            track_album_url.append(item['track']['album']['href'])
            
        track_album_artists_list = []
        for item in tracks_json['items']:
            track_album_artists_list.append(item['track']['album']['artists'])
        track_album_artists = []
        for artist in track_album_artists_list:
            track_album_artist_name = []
            for artist_name in artist:
                track_album_artist_name.append(artist_name['name'])
            track_album_artists.append(track_album_artist_name)
            
        track_album_artists_converted =[]
        for track in track_album_artists:
            track = '; '.join(track)
            track_album_artists_converted.append(track)
        spotify_songs_df = pd.DataFrame({
        'track_id': track_id,
        'track_name': track_name,
        'track_artists': track_artists_converted,
        'track_album_id':track_album_id,
        'track_album_name': track_album_name,
        'track_album_release_date': track_album_release_date,
        'track_album_total_tracks': track_album_total_tracks,
        'track_album_artists': track_album_artists_converted,
        'track_album_url':track_album_url
    })
        spotify_songs_df['track_album_name'] = spotify_songs_df['track_album_name'].str.replace(',',' ')
        # return spotify_songs_df
        csv_buffer = StringIO()
        spotify_songs_df.to_csv(csv_buffer,index=False)

        transform_data_file.append(csv_buffer.getvalue())

        file_name = "spotify_transformed_data" + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
        kwargs['ti'].xcom_push(key = 'transform_data_file_list', value=transform_data_file)
        kwargs['ti'].xcom_push(key = 'transform_data_file_name', value=file_name)
    

def upload_transformed_data_back_to_s3(**kwargs):
    transform_data_file = kwargs['ti'].xcom_pull(task_ids='process_data_from_s3', key='transform_data_file_list')
    transform_file_name =kwargs['ti'].xcom_pull(task_ids='process_data_from_s3', key='transform_data_file_name')
    s3hook = S3Hook(aws_conn_id = 'aws_default')

    bucket_name = "spotify-api-project2-mahak"
    object_key = "transformed_data/track_songs_data/" + transform_file_name
    for data in transform_data_file:
        s3hook.load_string(
            string_data = data,
            bucket_name = bucket_name,
            key = object_key,
            replace = True
        )

# copy the already processed json file from to_processed folder to processed folder.
# delete the file from to_processed folder so that they won't process again and again.
def copy_toprocessed_to_processed(**kwargs):
    spotify_key_file = kwargs['ti'].xcom_pull(task_ids='read_data_from_s3', key='spotify_key_file')
    bucket_name = "spotify-api-project2-mahak"
    prefix ="raw_data/to_processed/"
    target_prefix = "raw_data/processed/"
    
    if not spotify_key_file:
        raise ValueError("No files retrieved from XCom.")
    
    s3hook = S3Hook(aws_conn_id='aws_default')
    
    for file in spotify_key_file:
        source_key = file
        destination_key = file.replace(prefix,target_prefix)
        
        try:
            # Copy the file
            s3hook.copy_object(
                source_bucket_key=source_key,
                dest_bucket_key=destination_key,
                source_bucket_name=bucket_name,
                dest_bucket_name=bucket_name,
            )
            logging.info(f"Copied {source_key} to {destination_key}")
        
        except Exception as e:
            logging.error(f"Failed to copy {source_key} to {destination_key}: {str(e)}")
            continue
        
        try:
            # Delete the original file
            s3hook.delete_objects(bucket=bucket_name, keys=file)
            logging.info(f"Deleted {source_key}")
        
        except Exception as e:
            logging.error(f"Failed to delete {source_key}: {str(e)}")



with DAG(
    dag_id = 'spotify_data_api',
    default_args = default_args,
    start_date = datetime(2024,9,6),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    extract_spotify_api_data = PythonOperator(
        task_id = 'extract_data_from_spotify',
        python_callable = extract_data_from_spotify,
        provide_context=True
    )

    upload_spotify_raw_data_to_s3 = PythonOperator(
        task_id = 'upload_raw_data_to_s3',
        python_callable = upload_raw_data_to_s3,
        provide_context=True
    )
    read_raw_data_from_s3 = PythonOperator(
        task_id = 'read_data_from_s3',
        python_callable = read_data_from_s3,
        provide_context = True
    )

    transform_data_from_s3 = PythonOperator(
        task_id = 'process_data_from_s3',
        python_callable = process_data_from_s3,
        provide_context = True
    )

    upload_transform_data_to_s3 = PythonOperator(
        task_id = 'upload_transformed_data_back_to_s3',
        python_callable = upload_transformed_data_back_to_s3,
        provide_context = True
    )

    copy_file_from_toprocessed_to_processed = PythonOperator(
        task_id = 'copy_toprocessed_to_processed',
        python_callable = copy_toprocessed_to_processed,
        provide_context = True
    )



    extract_spotify_api_data >> upload_spotify_raw_data_to_s3
    upload_spotify_raw_data_to_s3  >> read_raw_data_from_s3
    read_raw_data_from_s3 >> transform_data_from_s3
    transform_data_from_s3 >> upload_transform_data_to_s3
    upload_transform_data_to_s3 >> copy_file_from_toprocessed_to_processed
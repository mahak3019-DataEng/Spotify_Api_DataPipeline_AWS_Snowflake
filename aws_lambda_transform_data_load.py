import json
import boto3
from datetime import datetime
import pandas as pd
from io import StringIO

def tracks_json_transform(tracks_json):
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
    return spotify_songs_df
        
def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    
    Bucket="spotify-api-project2-mahak"
    Key = "raw_data/to_processed/"
    
    # print(s3.list_objects(Bucket= Bucket, Prefix=Key)['Contents'])
    all_files = s3.list_objects(Bucket= Bucket, Prefix=Key)['Contents']
    
    spotify_data_file_list = []
    spotify_data_key_list = []
    for file in all_files:
        file_key = file['Key']
        if file['Key'].split('.')[-1] == 'json':
            response = s3.get_object(Bucket=Bucket, Key=file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data_file_list.append(jsonObject)
            spotify_data_key_list.append(file_key)
            
    
    # Function to transform the json data and upload it into csv format.
    # Function to store the json file which is already transformed into processed raw data in S3 bucket.
    for json_data in spotify_data_file_list:
        tracks_df = tracks_json_transform(json_data)
        csv_buffer = StringIO()
        tracks_df.to_csv(csv_buffer,index=False)
        
        filename = "spotify_transformed_data" + str(datetime.now()) + ".csv"
        s3.put_object(
            Bucket=Bucket,
            Key="transformed_data/track_songs_data/" + filename,
            Body=csv_buffer.getvalue()
        )
        
        processed_filename = "spotify_processed_raw"+ str(datetime.now()) + ".json"
        s3.put_object(
            Bucket= Bucket,
            Key = "raw_data/processed/" + processed_filename,
            Body=json.dumps(json_data)
            )
            
            
    # Function to delete the raw data file from to_processsed folder so that it is not transformed again and again.
    for key_file in spotify_data_key_list:
        s3.delete_object(
            Bucket = Bucket,
            Key= key_file
            )
        
        
            
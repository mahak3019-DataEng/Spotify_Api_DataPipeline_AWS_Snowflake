import json
import os
import base64
import requests
from requests import post, get
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # TODO implement
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
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
    # print("Access Token:", access_token)
    def get_auth_header(token):
        return {"Authorization": "Bearer " + token}

    # Creating the function to get the top 100 songs on Spotify.
    playlist_link = "https://open.spotify.com/playlist/4JZFUSM0jb3RauYuRPIUp8"
    playlist_id = playlist_link.split('/')[-1]  
    
    def get_playlist_tracks(access_token, playlist_id):
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
        headers = get_auth_header(access_token)
        result = get(url, headers=headers)
        json_result = json.loads(result.content)
        return json_result
    
    tracks_json = get_playlist_tracks(access_token, playlist_id)
    
    filename = "spotify_raw_" + str(datetime.now()) + ".json"
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket = "spotify-api-project2-mahak",
        Key = "raw_data/to_processed/" + filename,
        Body = json.dumps(tracks_json)
        )
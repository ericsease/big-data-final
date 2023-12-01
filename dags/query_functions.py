import json
import spotipy
import yaml
from spotipy.oauth2 import SpotifyClientCredentials


def track_id_crawl(client_id: str, client_secret: str, playlist_id: str,
                   file_name: str = "/home/airflow/airflow/dags/data/track_ids.json"):
    """
    Crawls through a Spotify playlist and retrieves track IDs and names,
    updating a JSON file with the collected data.

    Parameters:
    - client_id (str): Spotify API client ID for authentication.
    - client_secret (str): Spotify API client secret for authentication.
    - playlist_id (str): Spotify playlist ID from which to retrieve track information.
    - file_name (str): Optional. File name and path to save the track data in JSON format.
    Default is "data/track_ids.json".

    Returns:
    None
    """
    # spotify api authentication
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,
                                                          client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # get all tracks from playlist
    playlist_all_tracks = sp.playlist_tracks(playlist_id)

    # load general track_json from json
    with open(file_name, 'r') as json_file:
        track_json = json.load(json_file)

    # get track ids from all tracks
    for track_meta in playlist_all_tracks['items']:
        track_json[track_meta['track']['id']] = track_meta['track']['name']

    # save track_json to json
    with open(file_name, 'w') as json_file:
        json.dump(track_json, json_file, indent=None)


def save_track_ids():
    """
    Fetches track IDs from playlists specified in the 'playlist_ids.json' file and saves them.

    The function reads client credentials from 'config.yaml' and retrieves playlist IDs from
    'playlist_ids.json'. It then calls the 'track_id_crawl' function for each playlist ID to
    crawl and save track IDs.

    Note:
        Ensure that the 'config.yaml' file contains valid credentials in the 'credentials' section,
        and the 'playlist_ids.json' file is present with the required playlist IDs.

    Raises:
        FileNotFoundError: If either 'config.yaml' or 'playlist_ids.json' is not found.

    Example:
        save_track_ids()

    """
    print("we've entered track_ids!")
    # load credentials from config.yaml
    with open('/home/airflow/airflow/dags/config.yaml', 'r') as yaml_file:
        credentials = yaml.safe_load(yaml_file)

    client_id = credentials['credentials']['client_id']
    client_secret = credentials['credentials']['client_secret']

    # get playlist_json with ids
    with open("/home/airflow/airflow/dags/data/playlist_ids.json", 'r') as json_file:
        playlist_json = json.load(json_file)

    # crawl tracks from playlists
    for playlist_id in playlist_json.keys():
        track_id_crawl(client_id, client_secret, playlist_id)


def get_audio_features():
    """
    Retrieves audio features for specified track IDs using the Spotify API and saves them to a JSON file.

    The function loads Spotify API credentials from 'config.yaml', authenticates with the Spotify API,
    and fetches a list of track IDs from 'track_ids.json'. It then retrieves audio features for each
    track ID using the Spotify API and saves the results to 'audio_features.json'.

    Note:
        Ensure that the 'config.yaml' file contains valid credentials in the 'credentials' section,
        and the 'track_ids.json' file is present with the required track IDs.

    Raises:
        FileNotFoundError: If either 'config.yaml' or 'track_ids.json' is not found.

    Example:
        get_audio_features()

    """
    # load credentials from yaml file
    print("entered get_audio_features!")
    with open('/home/airflow/airflow/dags/config.yaml', 'r') as yaml_file:
        credentials = yaml.safe_load(yaml_file)

    client_id = credentials['credentials']['client_id']
    client_secret = credentials['credentials']['client_secret']

    # spotify api authentication
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,
                                                          client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    # load general track_json from json
    with open("/home/airflow/airflow/dags/data/track_ids.json", 'r') as json_file:
        tracks_json = json.load(json_file)

    print("loaded track_ids.json!")

    audio_feat = {}
    for track_id in tracks_json:
        audio_feat[track_id] = sp.audio_features(track_id)[0]

    # save track_json to json
    with open("/home/airflow/airflow/dags/data/audio_features.json", 'w') as json_file:
        json.dump(audio_feat, json_file, indent=None)

import dlt
import os
import duckdb
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from typing import Generator, List
from itertools import islice
from time import sleep
from numpy import ceil
from dotenv import load_dotenv

import constants

# Load environment variables
load_dotenv()

@dlt.source(max_table_nesting=0)
def spotify_source(track_ids: List[str]):
    "Uses dltHub's RESTClient to fetch Spotify track and artist data."
    
    client = RESTClient(
        base_url="https://api.spotify.com/v1",
        auth=OAuth2ClientCredentials(
            access_token_url="https://accounts.spotify.com/api/token",
            client_id=os.getenv("SPOTIFY_CLIENT_ID"),
            client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
            access_token_request_data={"Content-Type": "application/x-www-form-urlencoded"}
        )
    )

    @dlt.resource(table_name='raw_track', write_disposition="merge", primary_key="id")
    def tracks():
        
        response = client.get("/tracks", params={"ids": ",".join(track_ids)})
        
        # Check for a successful response before attempting to parse JSON
        if response.status_code == 200:
            full_response = response.json()
            yield full_response["tracks"]
        elif response.status_code == 429:
            raise Exception(f"{response.text}. Try after {response.headers['retry-after']} seconds.")
            
    @dlt.transformer(data_from=tracks, table_name="raw_artist", write_disposition="merge", primary_key="id")
    def artists(tracks):
        
        artist_ids = [track['artists'][0]['id'] for track in tracks]
        response = client.get("/artists", params={"ids": ",".join(artist_ids)})
        
        # Check for a successful response before attempting to parse JSON
        if response.status_code == 200:
            full_response = response.json()
            yield full_response["artists"]
        elif response.status_code == 429:
            raise Exception(f"{response.text}. Try after {response.headers['retry-after']} seconds.")  

    return tracks, artists

def load_spotify() -> None:
    
    pipeline = dlt.pipeline(
        destination="duckdb",
        pipeline_name="spotify",
        dataset_name="dev"
    )

    # Process tracks in batches of 50 (Spotify limit per request)
    batch_size = 50
    total_batches = int(ceil(len(track_ids) / batch_size))
    for batch_number, batch in enumerate(batch_track_ids(track_ids, batch_size), start=1):
        
        # Print batch information
        print(f"Processing batch {batch_number}/{total_batches}...")
        sleep(3)
        
        # Process the batch and print run info
        load_info = pipeline.run(spotify_source(batch))
        print(load_info)
    
def batch_track_ids(track_ids: List[str], batch_size: int) -> Generator[List[str], None, None]:
    """Helper function to yield batches of track IDs."""
    
    iterator = iter(track_ids)
    while batch := list(islice(iterator, batch_size)):
        yield batch

def get_track_ids() -> List[str]:
    """Extract track IDs from Spotify streaming history."""
    
    track_ids = duckdb.sql(
        f"""
        select
            distinct split(spotify_track_uri, ':')[3] as track_id

        from '{constants.STREAMING_HISTORY_FILE_PATH}'
                
        where spotify_track_uri is not null
        """
    ).pl()
    
    # Save track ids to CSV before returning
    track_ids.write_csv(constants.TRACK_IDS_FILE_PATH)
    
    return track_ids.to_series().to_list()

def write_raw_streaming_history() -> None:
    """Write raw streaming history data to DuckDB with PII removed."""
    
    conn = duckdb.connect('spotify.duckdb')
    
    conn.sql(
        f"""
        create table dev.raw_streaming_history as
        (
            select
                ts,
                platform,
                ms_played,
                conn_country,
                spotify_track_uri,
                reason_start,
                reason_end,
                shuffle,
                skipped,
                offline,
                incognito_mode
                
            from '{constants.STREAMING_HISTORY_FILE_PATH}'
        )
        """
    )

write_raw_streaming_history()
track_ids = get_track_ids()
load_spotify()
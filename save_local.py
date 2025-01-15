import duckdb
import constants

conn = duckdb.connect('spotify.duckdb')

# Save the raw, staging, and mart layers to disk
conn.sql(
    f"""
    copy dev.raw_streaming_history to '{constants.RAW_LAYER_FOLDER_PATH}/raw_streaming_history.json';
    copy dev.raw_track to '{constants.RAW_LAYER_FOLDER_PATH}/raw_track.json';
    copy dev.raw_artist to '{constants.RAW_LAYER_FOLDER_PATH}/raw_artist.json';
    """
)

conn.sql(
    f"""
    copy dev.stg_streaming_history to '{constants.STAGING_LAYER_FOLDER_PATH}/stg_streaming_history.json';
    copy dev.stg_track to '{constants.STAGING_LAYER_FOLDER_PATH}/stg_track.json';
    copy dev.stg_artist to '{constants.STAGING_LAYER_FOLDER_PATH}/stg_artist.json';
    copy dev.stg_album to '{constants.STAGING_LAYER_FOLDER_PATH}/stg_album.json';
    """
)

conn.sql(
    f"""
    copy dev.fct_stream to '{constants.MART_LAYER_FOLDER_PATH}/fct_stream.csv';
    copy dev.fct_artist_genres to '{constants.MART_LAYER_FOLDER_PATH}/fct_artist_genres.csv';
    copy dev.dim_track to '{constants.MART_LAYER_FOLDER_PATH}/dim_track.csv';
    copy dev.dim_artist to '{constants.MART_LAYER_FOLDER_PATH}/dim_artist.csv';
    copy dev.dim_album to '{constants.MART_LAYER_FOLDER_PATH}/dim_album.csv';
    """
)

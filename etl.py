import os
import glob
import psycopg2
import pandas as pd
import datetime
from sql_queries import *
from create_tables import *


def process_song_file(cur, filepath):
    """
    Process each json song file.

    Parameters:
    cur (psycopg2.cursor): Postgres cursor to sparkifydb
    filepath (str): Path to current json file to process

    Returns:
    None
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process each json log file.

    Parameters:
    cur (psycopg2.cursor): Postgres cursor to sparkifydb
    filepath (str): Path to current json file to process

    Returns:
    None
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page.eq("NextSong")]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday], axis=1)
    column_labels =  ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(data=time_data.values, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        songid, artistid = results if results else None, None

        """
        There should be one matching value according to https://knowledge.udacity.com/questions/48698
        There is not any log data for the song_id currently in the dataset in the Udacity workspace.
        song_id = SOZCTXZ12AB0182364
        grep -ir SOZCTXZ12AB0182364 data/*
        song_data/A/A/C/TRAACCG128F92E8A55.json:{"num_songs": 1, "artist_id": "AR5KOSW1187FB35FF4", "artist_latitude": 49.80388, "artist_longitude": 15.47491, "artist_location": "Dubai UAE", "artist_name": "Elena", "song_id": "SOZCTXZ12AB0182364", "title": "Setanta matins", "duration": 269.58322, "year": 0}
        """

        # Print out song_id if found (for debugging)
        if songid is not None:
            print(songid)
            print(artistid)

        # insert songplay record
        songplay_data = (datetime.datetime.fromtimestamp(row.ts/1000.0), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process both song and log json files.

    Parameters:
    cur (psycopg2.cursor): Postgres cursor to sparkifydb
    conn (psycopg2.connection): Postgres connection object ot sparkifydb
    filepath (str): Path to current json file to process
    func (str): Function to call (either "process_song_file" or "process_log_file"

    Returns:
    None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Process song and data files and load into sparkifydb.

    Parameters:
    None

    Returns:
    None
    """

    dbname = "sparkifydb"

    # Obtain local development environment Postgres connection settings
    if os.environ.get("UDACITY_DATA_ENGINEER_POSTGRES_INSTANCE"):
        host = os.environ.get("UDACITY_DATA_ENGINEER_POSTGRES_INSTANCE")
        user = os.environ.get("UDACITY_DATA_ENGINEER_POSTGRES_USER")
        password = os.environ.get("UDACITY_DATA_ENGINEER_POSTGRES_PASSWORD")
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(host, dbname, user, password))
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    # drop & recreate tables
    if cur or conn:
        try:
            drop_tables(cur,conn)
            create_tables(cur,conn)
        except:
            print("Error: Could not drop & create tables")

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()

# Modeled PostGres types based on http://millionsongdataset.com/pages/field-list
# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays
        (
          songplay_id VARCHAR,
          start_time TIMESTAMP NOT NULL,
          user_id INT,
          level VARCHAR NOT NULL,
          song_id VARCHAR NOT NULL,
          artist_id VARCHAR NOT NULL,
          session_id INT,
          location VARCHAR NOT NULL,
          user_agent VARCHAR NOT NULL,
          PRIMARY KEY (songplay_id, user_id, session_id)
        )
""")

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users
        (
          user_id INT PRIMARY KEY,
          first_name VARCHAR NOT NULL,
          last_name VARCHAR NOT NULL,
          gender CHAR(1) NOT NULL,
          level VARCHAR NOT NULL
        )
""")

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs
        (
          song_id VARCHAR PRIMARY KEY,
          title VARCHAR NOT NULL,
          artist_id VARCHAR NOT NULL,
          year SMALLINT NOT NULL,
          duration REAL NOT NULL
        )
""")

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists
        (
          artist_id VARCHAR PRIMARY KEY,
          name VARCHAR NOT NULL,
          location VARCHAR,
          latitude FLOAT,
          longitude FLOAT
        )
""")

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time
        (
          start_time TIMESTAMP PRIMARY KEY,
          hour SMALLINT NOT NULL,
          day SMALLINT NOT NULL,
          week SMALLINT NOT NULL,
          month SMALLINT NOT NULL,
          year SMALLINT NOT NULL,
          weekday SMALLINT NOT NULL
        )
""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (song_id)
        DO NOTHING;
""")

artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (artist_id)
        DO NOTHING;
""")

# Do nothing if existing record already exists.
time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (start_time)
        DO NOTHING;
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

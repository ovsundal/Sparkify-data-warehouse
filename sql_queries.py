import configparser



# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR(255),
    auth VARCHAR(255),
    firstName VARCHAR(255),
    gender CHAR,
    itemInSession INT,
    lastName VARCHAR(255),
    length decimal,
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(255),
    page VARCHAR(255),
    registration FLOAT,
    sessionId INT,
    song VARCHAR(255),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(255),
    userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INT,
    artist_id VARCHAR(255),
    artist_latitude decimal,
    artist_longitude decimal,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(255),
    title VARCHAR(255),
    duration decimal,
    year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time bigint,
user_id int,
level varchar(255),
song_id varchar(255),
artist_id varchar(255),
session_id int,
location varchar(255),
user_agent varchar(255)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
user_id int,
first_name varchar(255),
last_name varchar(255),
gender char,
level varchar(255)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
song_id varchar(255),
title varchar(255),
artist_id varchar(255),
year int,
duration float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
artist_id varchar(255),
name varchar(255),
location varchar(255),
latitude decimal,
longitude decimal
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
ts bigint,
hour int,
day int,
week int,
month int,
year int,
weekday int
);
""")

# STAGING TABLES

staging_events_copy = \
    (
    """ 
    COPY staging_events FROM {}
     iam_role {} FORMAT AS JSON 'auto' COMPUPDATE OFF REGION 'us-west-2';
    """
).format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"))

staging_songs_copy = (
    """ 
    COPY staging_songs FROM {}
     iam_role {} FORMAT AS JSON 'auto' COMPUPDATE OFF REGION 'us-west-2';
    """
).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT ts, userid, level, s.song_id, s.artist_id, sessionid, location, useragent FROM staging_events AS e
        LEFT OUTER JOIN staging_songs AS s ON e.song = s.title)
    """
)

user_table_insert = (
    """
    INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
    (SELECT userid, firstname, lastname, gender, level FROM staging_events )
    """
)

song_table_insert = (
    """
    INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
    (SELECT song_id, title, artist_id, year, duration from staging_songs)
    """
)

artist_table_insert = (
    """
    INSERT INTO dim_artists(artist_id, name, location, latitude, longitude)
    (SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs)
    """
)

time_table_insert = (
    """
    INSERT INTO dim_time(ts)
    (SELECT ts from staging_events)
    """
)

time_table_full_insert = ('INSERT INTO dim_time(ts, hour, day, week, month, year, weekday) VALUES(%s,%s,%s,%s,%s,%s,%s);')
select_time_table = ('SELECT ts FROM dim_time')
truncate_time_table = ('TRUNCATE TABLE dim_time')


# QUERY LISTS




create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, staging_events_table_create, staging_songs_table_create]
drop_table_queries = [
     staging_events_table_drop, staging_songs_table_drop,
     songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]

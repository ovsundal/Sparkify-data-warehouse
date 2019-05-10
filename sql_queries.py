import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR(255) NULL,
    auth VARCHAR(255) NOT NULL,
    firstName VARCHAR(255) NOT NULL,
    gender CHAR NOT NULL,
    itemInSession INT NOT NULL,
    lastName VARCHAR(255) NOT NULL,
    length FLOAT NOT NULL,
    level VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL,
    method VARCHAR(255) NOT NULL,
    page VARCHAR(255) NOT NULL,
    registration FLOAT NOT NULL,
    sessionId INT NOT NULL,
    song VARCHAR(255) NULL,
    status INT NOT NULL,
    ts timestamp NOT NULL,
    userAgent VARCHAR(255) NOT NULL,
    userId INT NOT NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INT NOT NULL,
    artist_id VARCHAR(255) NOT NULL,
    artist_latitude VARCHAR(255) NULL,
    artist_longitude VARCHAR(255) NULL,
    artist_location VARCHAR(255) NULL,
    artist_name VARCHAR(255) NULL,
    song_id VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    duration FLOAT NOT NULL,
    year INT NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time timestamp NOT NULL,
user_id int  NOT NULL,
level varchar(255) NOT NULL,
song_id varchar(255) NOT NULL,
artist_id varchar(255) NOT NULL,
session_id int NOT NULL,
location varchar(255) NOT NULL,
user_agent varchar(255) NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
user_id int PRIMARY KEY,
first_name varchar(255) NOT NULL,
last_name varchar(255) NOT NULL,
gender char NOT NULL,
level varchar(255) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
song_id varchar(255) PRIMARY KEY,
title varchar(255) NOT NULL,
artist_id varchar(255) NOT NULL,
year int NOT NULL,
duration float NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
artist_id varchar(255) NOT NULL,
name varchar(255) NOT NULL,
location varchar(255) NOT NULL,
latitude real NOT NULL,
longitude real NOT NULL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
timestamp timestamp,
hour int NOT NULL,
day int NOT NULL,
week int NOT NULL,
month int NOT NULL,
year int NOT NULL,
weekday int NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, staging_events_table_create, staging_songs_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

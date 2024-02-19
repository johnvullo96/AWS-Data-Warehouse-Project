import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config.get('S3', 'LOG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
S3_SONG_DATA = config.get('S3', 'SONG_DATA')
DWH_IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INT SORTKEY,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR(MAX),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(MAX),
    artist_name VARCHAR(MAX),
    song_id VARCHAR(MAX),
    title VARCHAR(MAX),
    duration REAL,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender CHAR(1),
    level VARCHAR
);

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT NOT NULL,
    duration DECIMAL NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude DECIMAL,
    longitude DECIMAL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times (
    start_time TIMESTAMP PRIMARY KEY,
    hour NUMERIC NOT NULL,
    day NUMERIC NOT NULL,
    week NUMERIC NOT NULL,
    month NUMERIC NOT NULL,
    year NUMERIC NOT NULL,
    weekday NUMERIC NOT NULL
 );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE '{}'
FORMAT AS json {}
REGION 'us-west-2';
""").format(
        S3_LOG_DATA,
        DWH_IAM_ROLE_ARN,
        S3_LOG_JSONPATH
        )

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE '{}'
JSON 'auto'
REGION 'us-west-2';
""").format(
        S3_SONG_DATA,
        DWH_IAM_ROLE_ARN,
        )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
     TIMESTAMP 'epoch' + (events.ts/1000 * INTERVAL '1 second'),
     events.userId,
     events.level,
     songs.song_id,
     songs.artist_id,
     events.sessionId,
     events.location,
     events.userAgent
FROM staging_events events
LEFT JOIN staging_songs songs ON (events.artist = songs.artist_name 
                            AND events.song = songs.title)
WHERE events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT 
    DISTINCT events.userId,
    events.firstName,
    events.lastName,
    events.gender,
    events.level
FROM staging_events events
WHERE events.userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT 
    DISTINCT songs.song_id,
    songs.title,
    songs.artist_id,
    songs.year,
    songs.duration
FROM staging_songs songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT 
    DISTINCT songs.artist_id,
    songs.artist_name,
    songs.artist_location,
    songs.artist_latitude,
    songs.artist_longitude
FROM staging_songs songs
""")

time_table_insert = ("""
INSERT INTO times (start_time, hour, day, week, month, year, weekday)
SELECT 
    DISTINCT songplays.start_time,
    EXTRACT(hour from songplays.start_time),
    EXTRACT(day from songplays.start_time),
    EXTRACT(week from songplays.start_time),
    EXTRACT(month from songplays.start_time),
    EXTRACT(year from songplays.start_time),
    EXTRACT(weekday from songplays.start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

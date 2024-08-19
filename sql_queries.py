import configparser
from utilities import create_iam_role, create_redshift_cluster

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config['IAM_ROLE']['ARN']  


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"  
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"  
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
    artist varchar, 
    auth varchar, 
    firstName varchar, 
    gender varchar, 
    itemInSession integer, 
    lastName varchar, 
    length numeric, 
    level varchar, 
    location varchar, 
    method varchar, 
    page varchar, 
    registration numeric, 
    sessionid integer, 
    song varchar, 
    status integer, 
    ts bigint, 
    userAgent varchar, 
    userid integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id varchar,
    title varchar,
    num_songs integer, 
    artist_id varchar, 
    artist_latitude numeric,
    artist_longitude numeric, 
    artist_location varchar, 
    artist_name varchar, 
    duration numeric, 
    year integer
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id integer IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp not null, 
    user_id varchar not null distkey, 
    level varchar,
    song_id varchar, 
    artist_id varchar,  
    session_id integer, 
    location varchar, 
    user_agent varchar
) diststyle key;
""")

user_table_create = ("""
CREATE TABLE users (
    user_id integer PRIMARY KEY sortkey,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar PRIMARY KEY sortkey, 
    title varchar,
    artist_id varchar distkey, 
    year integer, 
    duration numeric
) diststyle key;
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar PRIMARY KEY sortkey, 
    name varchar, 
    location varchar, 
    latitude numeric, 
    longitude numeric
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer distkey,
    weekday integer
) diststyle key ;
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'], 
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], 
            config['IAM_ROLE']['ARN'])

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
)
SELECT DISTINCT 
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    userid AS user_id,
    level,
    song_id,
    artist_id,
    sessionid AS session_id,
    location,
    userAgent AS user_agent
FROM staging_events
JOIN staging_songs
ON staging_events.song = staging_songs.title
AND staging_events.artist = staging_songs.artist_name
AND staging_events.length = staging_songs.duration
WHERE staging_events.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT 
    userid AS user_id,
    firstName as first_name, 
    lastName as last_name, 
    gender,
    level
FROM staging_events
WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, 
    title,
    artist_id, 
    year, 
    duration
)
SELECT DISTINCT 
    song_id,
    title, 
    artist_id, 
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
SELECT DISTINCT 
    artist_id,
    artist_name AS name, 
    artist_location as location, 
    artist_latitude as latitude, 
    artist_longitude as longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    start_time, 
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dow FROM start_time) AS weekday
FROM (
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events
);
""")

# validation queries
# table size
tables = ['staging_songs', 'staging_events', 'songplays', 'users', 'songs', 'artists', 'time']
table_size_queries = [(f"""select count(*) from {table}""") for table in tables]

# check for duplicates 
keys = ['songplay_id', 'user_id', 'song_id', 'artist_id', 'start_time']
duplicate_queries = [(f"""SELECT 
                            {key}, 
                            COUNT(*) AS cnt
                        FROM 
                            {table}
                        GROUP BY 
                            {key}
                        HAVING 
                            COUNT(*) > 1
                        ORDER BY 
                            cnt DESC
                        LIMIT 5;""") for key, table in zip(keys, tables[2:])]



# QUERY LISTS
create_table_queries = [
    staging_events_table_create, staging_songs_table_create, 
    songplay_table_create, user_table_create, 
    song_table_create, artist_table_create, time_table_create
]
drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop, 
    songplay_table_drop, user_table_drop, 
    song_table_drop, artist_table_drop, time_table_drop
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert, user_table_insert, 
    song_table_insert, artist_table_insert, time_table_insert
]

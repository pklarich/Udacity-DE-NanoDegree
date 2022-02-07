import configparser

# CONFIGURATION

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
staging_song_table_drop = "DROP TABLE IF EXISTS staging_song"
staging_log_table_drop = "DROP TABLE IF EXISTS staging_log"

# CREATE TABLES

staging_song_data_create= ("""
CREATE TABLE IF NOT EXISTS staging_song(
artist_id varchar(300),
artist_latitude numeric,
artist_location varchar(300),
artist_longitude numeric,
artist_name varchar(300),
duration numeric,
song_id varchar(300),
num_songs int,
title varchar(300),
year int
)
""")

staging_log_data_create = ("""
CREATE TABLE IF NOT EXISTS staging_log(
artist varchar(300),
auth varchar(50),
firstName varchar(300),
gender varchar(5),
itemInSession int,
lastname varchar(300),
length numeric,
level varchar(50),
location varchar(300),
method varchar(50),
page varchar(50),
registration varchar(300),
sessionId int,
song varchar(300),
status int,
ts bigint,
userAgent varchar(300),
userId int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time datetime NOT NULL SORTKEY,
user_id int NOT NULL DISTKEY, 
level varchar(50) NOT NULL,
song_id varchar(300),
artist_id varchar(300),
session_id int NOT NULL,
location varchar(300),
user_agent varchar(300) NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id int PRIMARY KEY,
first_name varchar(300) NOT NULL,
last_name varchar(300) NOT NULL,
gender varchar(5),
level varchar(50) NOT NULL
)
DISTSTYLE ALL
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
song_id varchar(300) PRIMARY KEY,
song_title varchar(300) NOT NULL,
artist_id varchar(300) NOT NULL,
year int SORTKEY,
duration numeric NOT NULL
)
DISTSTYLE ALL
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id varchar(300) PRIMARY KEY,
artist_name varchar(300) NOT NULL,
artist_location varchar(300),
latitude numeric,
longitude numeric
)
DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time datetime SORTKEY DISTKEY PRIMARY KEY,
hour int NOT NULL,
day int NOT NULL,
week int NOT NULL,
month int NOT NULL,
year int NOT NULL,
weekday int NOT NULL
)
""")

# COPY DATA TO STAGING TABLES

staging_log_copy = ("""
COPY staging_log
FROM {}
IAM_ROLE {} 
FORMAT AS JSON {} 
REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_song_copy = ("""
COPY staging_song
FROM {}
IAM_ROLE {}  
FORMAT AS JSON 'auto' 
REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(
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
TIMESTAMP 'epoch' + (sl.ts/1000 * INTERVAL '1 second') AS start_time,
sl.userId AS user_id,
sl.level,
ss.song_id,
ss.artist_id,
sl.sessionId AS session_id,
sl.location,
sl.userAgent
FROM staging_log AS sl
INNER JOIN staging_song AS ss ON ss.title = sl.song
AND ss.artist_name = sl.artist
AND ss.title = sl.song
WHERE sl.page = 'NextSong' ;
""")

user_table_insert = ("""
INSERT INTO users(
user_id,
first_name, 
last_name, 
gender, 
level
) 
SELECT DISTINCT
sl.userId AS user_id,
sl.firstName AS first_name,
sl.lastName AS last_name,
sl.gender,
sl.level
FROM staging_log AS sl
WHERE sl.userId IS NOT NULL
AND sl.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(
song_id, 
song_title, 
artist_id, 
year, 
duration
) 
SELECT DISTINCT
ss.song_id,
ss.title AS song_title,
ss.artist_id,
ss.year,
ss.duration
FROM staging_song AS ss
WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(
artist_id, 
artist_name, 
artist_location, 
latitude, 
longitude
) 
SELECT DISTINCT
ss.artist_id,
ss.artist_name,
ss.artist_location,
ss.artist_latitude as latitude,
ss.artist_longitude as longitude
FROM staging_song AS ss
WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year, weekday)
SELECT DISTINCT
sl.start_time,
EXTRACT(hour from start_time) AS hour,
EXTRACT(day from start_time) AS day,
EXTRACT(week from start_time) AS week,
EXTRACT(month from start_time) AS month,
EXTRACT(year from start_time) AS year,
EXTRACT(weekday from start_time) AS weekday
FROM songplays AS sl
WHERE sl.ts IS NOT NULL;
""")


# QUERY LISTS

create_table_queries = [time_table_create, artist_table_create, user_table_create, song_table_create, songplay_table_create, staging_log_data_create, staging_song_data_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, staging_song_table_drop, staging_log_table_drop]
insert_table_queries = [time_table_insert, artist_table_insert, user_table_insert, song_table_insert, songplay_table_insert]
staging_copy_queries = [staging_log_copy, staging_song_copy]
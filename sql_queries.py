import configparser


# load config
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE if exists staging_events;"
staging_songs_table_drop = "DROP TABLE if exists staging_songs;"
songplay_table_drop = "DROP TABLE if exists songplays;"
user_table_drop = "DROP TABLE if exists  users;"
song_table_drop = "DROP TABLE if exists  songs;"
artist_table_drop = "DROP TABLE if exists  artists;"
time_table_drop = "DROP TABLE if exists  time;"

# CREATE TABLES

# create table staging_events
staging_events_table_create= ("""
CREATE TABLE staging_events
  (
     artist        VARCHAR,
     auth          VARCHAR,
     firstname     VARCHAR,
     gender        VARCHAR,
     iteminsession INT,
     lastname      VARCHAR,
     length        VARCHAR,
     level         VARCHAR,
     location      VARCHAR,
     method        VARCHAR,
     page          VARCHAR NOT NULL,
     registration  float,
     sessionid     INT,
     song          VARCHAR,
     status        INT,
     ts            BIGINT,
     useragent     VARCHAR,
     userid        VARCHAR
  ) ;
""")
# create table staging_songs
staging_songs_table_create = ("""
CREATE TABLE staging_songs
  (
     num_songs        BIGINT NOT NULL,
     artist_id        VARCHAR NOT NULL,
     artist_latitude  FLOAT,
     artist_longitude FLOAT,
     artist_location  VARCHAR,
     artist_name      VARCHAR,
     song_id          VARCHAR,
     title            VARCHAR,
     duration         FLOAT,
     year             INT
  ); 

""")
# create table fact songplay,  with user_id as distkey and start_time as sortkey
songplay_table_create = ("""
CREATE TABLE songplays
  (
     songplay_id BIGINT GENERATED ALWAYS AS IDENTITY(0,1) PRIMARY KEY ,
     start_time  VARCHAR NOT NULL sortkey,
     user_id     INT NOT NULL distkey,
     level       VARCHAR NOT NULL,
     song_id     VARCHAR,
     artist_id   VARCHAR ,
     session_id  VARCHAR,
     location    VARCHAR,
     user_agent  VARCHAR
  ); 
""")
# create table dimentions users, with user_id as sortkey
user_table_create = ("""
CREATE TABLE users
  (
     user_id    int NOT NULL PRIMARY KEY sortkey,
     first_name VARCHAR NOT NULL,
     last_name  VARCHAR NOT NULL,
     gender     VARCHAR NOT NULL,
     level      VARCHAR NOT NULL 
  ); 
""")
# create table dimentions songs, with song_id as sortkey
song_table_create = ("""
CREATE TABLE songs
  (
     song_id   VARCHAR NOT NULL PRIMARY KEY sortkey,
     title     VARCHAR NOT NULL,
     artist_id VARCHAR NOT NULL,
     year      INT NOT NULL,
     duration  FLOAT NOT NULL
  ); 
""")
# create table dimentions artists, with artist_id as sortkey
artist_table_create = ("""
CREATE TABLE artists
  (
     artist_id VARCHAR NOT NULL PRIMARY KEY sortkey,
     name      VARCHAR NOT NULL,
     location  VARCHAR,
     latitude  FLOAT,
     longitude FLOAT
  ); 
""")
# create table dimentions time, with start_time as sortkey
time_table_create = ("""
CREATE TABLE time
  (
     start_time VARCHAR NOT NULL PRIMARY KEY sortkey,
     hour       INT NOT NULL,
     day        INT NOT NULL,
     week       INT NOT NULL,
     month      INT NOT NULL,
     year       INT NOT NULL,
     weekday    INT NOT NULL
  );
""")

# STAGING TABLES

# load data to table staging_events from S3 aws
staging_events_copy = ("""
      copy staging_events from {}
      credentials 'aws_iam_role={}'
      region 'us-west-2'
      json {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

# load data to table staging_songs from S3 aws
staging_songs_copy = ("""
      copy staging_songs from {}
      credentials 'aws_iam_role={}'
      format as json 'auto'
      region 'us-west-2';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES
# Insert songplay table from staging_events and dimentions tables
songplay_table_insert = ("""
INSERT INTO songplays
            (
                        start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent
            )
SELECT (timestamp 'epoch' + ts/1000 * interval '1 second')::varchar,
       userid::int,
       level,
       song_id,
       artist_id,
       sessionid,
       location,
       useragent
FROM   staging_events a
JOIN   staging_songs b
ON     a.artist = b.artist_name AND    a.length=b.duration 
WHERE page='NextSong';
""")
# Insert users table from staging_events where page='NextSong'
user_table_insert = ("""
INSERT INTO users
SELECT distinct userid::int,
       firstname,
       lastname,
       gender,
       level
FROM   staging_events where page='NextSong'
""")

# Insert songs table from staging_songs
song_table_insert = ("""
INSERT INTO songs
SELECT distinct song_id,
       title,
       artist_id,
       year,
       duration
FROM   staging_songs; 
""")

# Insert artists table from staging_songs
artist_table_insert = ("""
INSERT INTO artists
SELECT distinct artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude
FROM   staging_songs
""")

# Insert time table from staging_events where page='NextSong'
time_table_insert = ("""
INSERT INTO time
SELECT distinct (timestamp 'epoch' + ts/1000 * interval '1 second')::varchar,
       extract (hour FROM timestamp 'epoch'  + ts/1000 * interval '1 second'),
       extract (day FROM timestamp 'epoch'   + ts/1000 * interval '1 second'),
       extract (week FROM timestamp 'epoch'  + ts/1000 * interval '1 second'),
       extract (month FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
       extract (year FROM timestamp 'epoch'  + ts/1000 * interval '1 second'),
       extract (dow FROM timestamp 'epoch'   + ts/1000 * interval '1 second')
FROM   staging_events where page='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]

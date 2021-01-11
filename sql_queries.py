import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATAPATH = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATAPATH= config.get('S3','SONG_DATA')

DWH_ROLE_ARN=config.get('IAM_ROLE','ARN') 

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS songs;"
artist_table_drop         = "DROP TABLE IF EXISTS artists;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# CREATE TABLES


staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist    varchar(200),
    auth      varchar,
    firstName varchar(60),
    gender    varchar(5),
    itemInSession varchar(100),
    lastName  varchar(60),
    length     varchar(20),
    level varchar(40),
    location varchar(300),
    method varchar(15),
    page varchar(200),
    registration real,
    sessionId bigint,
    song varchar(300),
    status bigint,
    ts  TIMESTAMP,
    userAgent varchar(300),
    userId varchar(100)
 );
""")

staging_songs_table_create = ("""
 CREATE TABLE staging_songs(
    num_songs int,
    artist_id text,
    artist_latitude real,
    artist_longitude real,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration numeric,
    year int
 );
""")

songplay_table_create = ("""
CREATE TABLE songplays(
     songplay_id int identity(1, 1),
     start_time time not null,
     user_id varchar(40) not null,
     level varchar(10) not null,
     song_id varchar(40)  not null,
     artist_id varchar(40) not null,
     session_id integer not null,
     location varchar(100),
     user_agent varchar(300) not null,
     primary key(songplay_id),
     foreign  KEY(user_id) references  users(user_id),
     foreign  KEY(song_id) references  songs(song_id),
     foreign  KEY(artist_id) references artists(artist_id)
 );
""")

user_table_create = ("""
CREATE TABLE users (
  user_id varchar(40) not null,
  first_name varchar(40),
  last_name varchar(40) null,
  gender varchar(2)     null,
  level varchar(10) not null,
  primary key(user_id)
 );
""")

song_table_create = ("""
CREATE TABLE songs (
 song_id varchar(40) not null,
 title varchar(2000) not null,
 artist_id varchar(40) not null,
 year int not null,
 duration real not null,
primary key(song_id)
 );
""")

artist_table_create = ("""
CREATE TABLE artists(
     artist_id varchar(40) not null,
     name varchar(500) not null,
     location varchar(500),
     latitude real  null,
     longitude real  null,
     primary key(artist_id)
 );
""")

time_table_create = ("""
  CREATE TABLE time(
    time_id int not null identity(1,1),
    start_time time not null,
    hour int not null,
    day int not null,
    week int not null, 
    month int not null,
    year int not null,
    weekday varchar(20) not null,
    primary key(time_id)
);
""")


# STAGING TABLES

staging_events_copy = ("""
  COPY staging_events  FROM {}
  credentials 'aws_iam_role={}'
  compupdate off region 'us-west-2'
  TIMEFORMAT as 'epochmillisecs'
  json {};
""").format(LOG_DATAPATH,DWH_ROLE_ARN,LOG_JSONPATH)


staging_songs_copy = ("""
 COPY staging_songs FROM  {}
  credentials 'aws_iam_role={}'
  compupdate off region 'us-west-2'
  json 'auto' ;
""").format(SONG_DATAPATH,DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT CAST(split_part(ts,' ',2) as time) as mytime,userid,level,song_id,artist_id,sessionid,location,useragent
FROM staging_events se
JOIN staging_songs so  ON so.artist_name = se.artist

 ;
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT DISTINCT userid,firstname,lastname,gender,level
FROM staging_events 
WHERE userid is not null
;
 
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT DISTINCT song_id,title,artist_id,year,duration
FROM staging_songs
WHERE song_id is not null
 ;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id,name,location,latitude,longitude)
SELECT DISTINCT artist_id,artist_name,artist_location,artist_latitude,artist_longitude
FROM  staging_songs
WHERE artist_id is not null
 ;
""")

time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT Cast(split_part(ts,' ',2) as time) as mytime,cast(split_part(split_part(ts,' ',2) ,':',1) as int) as hour,
cast(split_part(split_part(ts,'-',3),' ',1) as int) as day , cast(date_part(w,ts) as int) as week, cast(split_part(ts,'-',2) as int) as month, cast(split_part(ts,'-',1) as int) as year,to_char(ts, 'Day') as dayofweek
FROM staging_events
WHERE ts is not null
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, time_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create]
drop_table_queries   = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries   = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

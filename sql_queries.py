import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

dbuser= config.get("CLUSTER","DB_USER")
ARN = config.get("IAM_ROLE","ARN")
song_path = config.get("S3","SONG_DATA")
log_path = config.get("S3","LOG_DATA")
log_jsonpath = config.get("S3","LOG_JSONPATH")

# DROP SCHEMA
drop_redshift_schema ="DROP SCHEMA IF EXISTS sparkifydw cascade"

# CREATE SCHEMA
create_redshift_schema ="CREATE SCHEMA IF NOT EXISTS sparkifydw authorization " + dbuser 

# SET SCHEMA PATH
set_schema = "SET search_path TO sparkifydw;"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(
	artist varchar,
	auth varchar,
	firstName varchar,
	gender varchar,
	iteminsession int,
	lastname varchar,
	length numeric,
	level varchar,
	location varchar,
	method varchar,
	page varchar,
	registration numeric,
	sessionid int,
	song varchar,
	status int,
	ts numeric,
	userAgent varchar,
	userid int
) diststyle all;
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(num_songs int,
 artist_id varchar(max),
 artist_latitude numeric,
 artist_longitude numeric,
 artist_location varchar(max),
 artist_name varchar(max),
 song_id  varchar(max),
 title	varchar(max),
 duration numeric,
 year int
) diststyle even;
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id int identity(0, 1) PRIMARY KEY unique not null , start_time timestamp null , user_id int , song_id varchar , artist_id varchar, session_id int , location varchar, user_agent varchar, level varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id int PRIMARY KEY unique not null , first_name varchar not null, last_name varchar not null, gender varchar, level varchar) diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY unique not null sortkey, title varchar not null,artist_id varchar not null ,year int, duration numeric) diststyle all
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY unique not null sortkey, name varchar not null, location varchar , latitude numeric, longitude numeric) diststyle all;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp PRIMARY KEY unique not null sortkey, hour int not null, day int not null, week int not null, month int not null, year int not null, weekday int not null) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
    iam_role {}
    json {} compupdate off region 'us-west-2';
""").format(log_path,ARN,log_jsonpath)

staging_songs_copy = ("""copy staging_songs from {}
    iam_role {}
    json 'auto' compupdate off region 'us-west-2';
""").format(song_path,ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct
	'1970-01-01' ::date + (ts/1000) * interval '1 second' as start_time,
	se.userid,
	se.level,
	s.song_id,
	a.artist_id,
	se.sessionid,
	se.location,
	se.userAgent
from	
	songs s
	left join artists a on a.artist_id = s.artist_id
	left join staging_events se
		on se.song = s.title and se.artist = a.name and se.length = s.duration;
""")

user_table_insert = ("""
    insert into users
    select distinct	
        userid,
        firstname,
        lastname,
        gender,
        level
    from
        staging_events
    where userid is not null;
""")

song_table_insert = ("""insert into songs
                            select distinct	
                                song_id,
                                title,
                                artist_id,
                                year,
                                duration
                            from
                                staging_songs;
""")

artist_table_insert = ("""insert into artists
                            select distinct
                                artist_id,
                                artist_name,
                                artist_location,
                                artist_longitude,
                                artist_latitude
                            from
                                staging_songs;
""")

time_table_insert = ("""
insert into time
select 
	'1970-01-01' ::date + (ts/1000) * interval '1 second' as start_time ,
     extract(hour from ('1970-01-01' ::date + (ts/1000) * interval '1 second')) as hour,
     extract(day from ('1970-01-01' ::date + (ts/1000) * interval '1 second')) as day,
     extract(week from ('1970-01-01' ::date + (ts/1000) * interval '1 second')) as week,
     extract(month from ('1970-01-01' ::date + (ts/1000) * interval '1 second')) as month,
     extract(year from ('1970-01-01' ::date + (ts/1000) * interval '1 second')) as year,
     extract(weekday from ('1970-01-01' ::date + (ts/1000) * interval '1 second')) as weekday
from sparkifydw.staging_events;	
""")

# QUERY LISTS

create_table_queries = [set_schema, staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [set_schema, staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [set_schema, staging_events_copy, staging_songs_copy]
insert_table_queries = [set_schema, user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
create_schema_query = [create_redshift_schema, set_schema]
drop_schema_query = [drop_redshift_schema]
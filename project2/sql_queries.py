import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
  artist varchar(265),
  auth   varchar(30),
  firstName varchar(30),
  gender varchar(30),
  itemInSession integer NOT NULL,
  lastName varchar(30),
  length real,
  level varchar(30),
  location varchar(265),
  method varchar(30),
  page varchar(30),
  registration real,
  sessionId integer NOT NULL SORTKEY,
  song varchar(265),
  status integer,
  ts bigint,
  userAgent varchar(265),
  userId varchar(10) NOT NULL
);
""")


staging_songs_table_create = ("""
CREATE TABLE staging_songs (
  artist_id varchar(60) NOT NULL SORTKEY,
  artist_latitude real,
  artist_location varchar(265),
  artist_longitude real,
  artist_name varchar(265),
  duration real,
  num_songs integer,
  song_id varchar(60) NOT NULL,
  title varchar(265),
  year integer
);
""")
                              
"""
### Fact Table

1. **songplays** - records in event data associated with song plays i.e. records with page `NextSong`
    - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

songplay_id - autogenerate
join staging_events on song
level
userId for user_id
sessionId for session_id
ts for start_time
location
userAgent for user_Agent
join staging_songs on title
song_id
artist_id

### Dimension Tables

1. **users** - users in the app
    - *user_id, first_name, last_name, gender, level* all from staging_events
2. **songs** - songs in music database
    - *song_id, title, artist_id, year, duration* all from staging_songs
3. **artists** - artists in music database
    - *artist_id, name, location, lattitude, longitude* all from staging_songs
4. **time** - timestamps of records in **songplays** broken down into specific units
    - *start_time, hour, day, week, month, year, weekday* all from songplays

### Sample Join
select le.starttime, d.query, d.line_number, d.colname, d.value,
le.raw_line, le.err_reason    
from stl_loaderror_detail d, stl_load_errors le
where d.query = le.query
order by le.starttime desc
limit 100

"""

songplay_table_create = ("""
CREATE TABLE songplays (
  songplay_id integer identity(0,1) sortkey,
  start_time bigint,
  user_id varchar(10),
  level varchar(30),
  song_id varchar(60),
  artist_id varchar(60),
  session_id integer,
  location varchar(265),
  user_agent varchar(265)
);
""")

user_table_create = ("""
CREATE TABLE users (
  user_id varchar(10) sortkey,
  first_name varchar(30),
  last_name varchar(30),
  gender varchar(30),
  level varchar(30)
);
""")

song_table_create = ("""
CREATE TABLE songs (
  song_id varchar(60) sortkey,
  title varchar(265),
  artist_id varchar(60),
  year integer,
  duration real
);
""")

artist_table_create = ("""
CREATE TABLE artists (
  artist_id varchar(60) sortkey,
  name varchar(265),
  location varchar(265),
  latitude real,
  longitude real
);
""")

time_table_create = ("""
CREATE TABLE time (
  start_time bigint,
  hour integer,
  day integer,
  week integer,
  month varchar(9),
  year integer,
  weekday varchar(9)
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY {} from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json {};
""").format("staging_events",config['S3'].get('LOG_DATA'),config['IAM_ROLE'].get('ARN'), config['S3'].get('LOG_JSONPATH'))

staging_songs_copy = ("""
COPY {} from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto';
""").format("staging_songs",config['S3'].get('SONG_DATA'),config['IAM_ROLE'].get('ARN'))

# FINAL TABLES
"""
  start_time bigint,
  user_id varchar(10),
  level varchar(30),
  song_id varchar(60),
  artist_id varchar(60),
  session_id integer,
  location varchar(265),
  user_agent varchar(265)
  
  updated query: select count (*) from staging_events ev left join staging_songs so on ev.song = so.title AND ev.artist = so.artist_name where ev.page = 'NextSong'
"""
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(select ev.ts, ev.userId, ev.level, so.song_id, so.artist_id, ev.sessionId, ev.location, ev.userAgent from staging_events ev, staging_songs so where ev.song = so.title);
""")

"""
  artist varchar(265),
  auth   varchar(30),
  firstName varchar(30),
  gender varchar(30),
  itemInSession integer NOT NULL,
  lastName varchar(30),
  length real,
  level varchar(30),
  location varchar(265),
  method varchar(30),
  page varchar(30),
  registration real,
  sessionId integer NOT NULL SORTKEY,
  song varchar(265),
  status integer,
  ts bigint,
  userAgent varchar(265),
  userId varchar(10) NOT NULL

  1. **users** - users in the app
    - *user_id, first_name, last_name, gender, level* all from staging_events

  artist_id varchar(60) NOT NULL SORTKEY,
  artist_latitude real,
  artist_location varchar(265),
  artist_longitude real,
  artist_name varchar(265),
  duration real,
  num_songs integer,
  song_id varchar(60) NOT NULL,
  title varchar(265),
  year integer
2. **songs** - songs in music database
    - *song_id, title, artist_id, year, duration* all from staging_songs
3. **artists** - artists in music database
    - *artist_id, name, location, lattitude, longitude* all from staging_songs

"""

user_table_insert = ("""
insert into users
(select userId, firstName, lastName, gender, level from staging_events);
""")

song_table_insert = ("""
insert into songs
(select song_id, title, artist_id, year, duration from staging_songs);
""")

artist_table_insert = ("""
insert into artists
(select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs);
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert] #, time_table_insert]

# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

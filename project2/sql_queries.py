import configparser

# Load Config
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Drop Tables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# Create Staging Tables
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

# Create Fact Table
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
                         
# Create Dimension Tables
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
  start_time timestamp,
  hour integer,
  day integer,
  week integer,
  month varchar(9),
  year integer,
  weekday varchar(9)
);
""")

# Populate Staging Tables
staging_events_copy = ("""
COPY {} FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON {};
""").format("staging_events",config['S3'].get('LOG_DATA'),config['IAM_ROLE'].get('ARN'), config['S3'].get('LOG_JSONPATH'))

staging_songs_copy = ("""
COPY {} FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""").format("staging_songs",config['S3'].get('SONG_DATA'),config['IAM_ROLE'].get('ARN'))

# Populate Fact Table
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(SELECT ev.ts, ev.userId, ev.level, so.song_id, so.artist_id, ev.sessionId, ev.location, ev.userAgent FROM staging_events ev LEFT JOIN staging_songs so ON ev.song = so.title AND ev.artist = so.artist_name WHERE ev.page = 'NextSong');
""")

# Populate Dimension Tables
user_table_insert = ("""
INSERT INTO users
(SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events);
""")

song_table_insert = ("""
INSERT INTO songs 
(SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs);
""")

artist_table_insert = ("""
INSERT INTO artists
(SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs);
""")

time_table_insert = ("""
INSERT INTO time WITH #epochconvert AS (SELECT TIMESTAMP 'epoch' + start_time/1000 * INTERVAL '1 second' as start_time FROM songplays)
SELECT DISTINCT start_time, EXTRACT(HOUR FROM start_time), EXTRACT(DAY FROM start_time), EXTRACT(WEEK FROM start_time), EXTRACT(YEAR FROM start_time), EXTRACT(WEEKDAY FROM start_time) FROM #epochconvert
""")

# Query Lists to Execute in create_tables.py and etl.py
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

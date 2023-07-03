import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
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
  userAgent varchar(200),
  userId varchar(10) NOT NULL
);
""")
                              
#   itemInSession integer, sort
#   sessionId integer, primary
#   userId varchar(10) sort

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
  num_songs integer,
  artist_id varchar(60) NOT NULL SORTKEY,
  artist_latitude real,
  artist_longitude real,
  artist_location varchar(265),
  artist_name varchar(265),
  song_id varchar(60) NOT NULL,
  title varchar(265)
)
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
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

create_table_queries = [staging_events_table_create, staging_songs_table_create]#, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

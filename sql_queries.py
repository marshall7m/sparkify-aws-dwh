import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# used for staging table COPY commands
LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("CLUSTER","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
    (   
        artist          VARCHAR,
        auth            VARCHAR, 
        firstName       VARCHAR,
        gender          VARCHAR,   
        itemInSession   INTEGER,
        lastName        VARCHAR,
        length          FLOAT,
        level           VARCHAR, 
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    BIGINT,
        sessionId       INTEGER,
        song            VARCHAR,
        status          INTEGER,
        ts              TIMESTAMP,
        userAgent       VARCHAR,
        userId          INTEGER
    )
""")


staging_songs_table_create = ("""
CREATE TABLE staging_songs
    (
        song_id            VARCHAR,
        num_songs          INTEGER,
        title              VARCHAR,
        artist_name        VARCHAR,
        artist_latitude    FLOAT,
        year               INTEGER,
        duration           FLOAT,
        artist_id          VARCHAR,
        artist_longitude   FLOAT,
        artist_location    VARCHAR
    )
""")

songplay_table_create = (""" 
CREATE TABLE fact_songplay 
    (
        songplay_id   INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time    TIMESTAMP, 
        user_id       VARCHAR,
        level         VARCHAR,
        song_id       VARCHAR, 
        artist_id     VARCHAR, 
        session_id    INTEGER, 
        location      VARCHAR,
        user_agent    VARCHAR
    )
""")

user_table_create = (""" 
CREATE TABLE dim_users 
    (
        user_id    VARCHAR PRIMARY KEY,
        first_name VARCHAR, 
        last_name  VARCHAR, 
        gender     VARCHAR, 
        level      VARCHAR
    )
diststyle all
""")

song_table_create = (""" 
CREATE TABLE dim_songs 
    (
        song_id   VARCHAR PRIMARY KEY,
        title     VARCHAR, 
        artist_id VARCHAR DISTKEY, 
        year      INTEGER, 
        duration  FLOAT
    )
""")

artist_table_create = (""" 
CREATE TABLE dim_artists 
    (
        artist_id VARCHAR PRIMARY KEY,
        name      VARCHAR, 
        location  VARCHAR DISTKEY, 
        latitude  FLOAT, 
        longitude FLOAT
    )
""")

time_table_create = (""" 
CREATE TABLE dim_time 
    (
        start_time TIMESTAMP PRIMARY KEY SORTKEY, 
        hour       INTEGER, 
        day        INTEGER,
        week       INTEGER, 
        month      INTEGER, 
        year       INTEGER, 
        weekday    INTEGER
    )
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
BLANKSASNULL EMPTYASNULL
TIMEFORMAT as 'epochmillisecs'
FORMAT AS JSON {};
""").format(LOG_DATA, ARN, LOG_PATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON 'auto'  
BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = (""" 
INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    e.ts, 
    e.userId,
    e.level,
    s.song_id, 
    s.artist_id, 
    e.sessionId, 
    s.artist_location,
    e.userAgent
FROM staging_events e
JOIN staging_songs s
ON (e.artist = s.artist_name) AND (e.song = s.title)
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId,
    firstName, 
    lastName, 
    gender, 
    level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    ts, 
    EXTRACT(HOUR FROM ts), 
    EXTRACT(DAY FROM ts),
    EXTRACT(WEEK FROM ts), 
    EXTRACT(MONTH FROM ts), 
    EXTRACT(YEAR FROM ts), 
    EXTRACT(WEEKDAY FROM ts)
FROM staging_events
WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = {
    'fact_songplay': songplay_table_insert, 
    'dim_user': user_table_insert, 
    'dim_songs': song_table_insert,
    'dim_artists': artist_table_insert, 
    'dim_time': time_table_insert
}

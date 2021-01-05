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
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar(200) NULL,
        auth varchar(10) NULL,
        firstName varchar(20) NULL,
        gender char(1) NULL,
        itemInSession int NULL,
        lastName varchar(50) NULL,
        length numeric NULL,
        level char(4) NULL,
        location varchar(50) NULL,
        method char(3) NULL,
        page varchar(30) NULL,
        registration bigint NULL,
        sessionId int NULL,
        song varchar(200) NULL,
        status char(3) NULL,
        ts bigint NULL,
        userAgent varchar(200) NULL,
        userId int NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int NULL,
        artist_id varchar(18) NULL,
        artist_latitude varchar(30) NULL,
        artist_longitude varchar(30) NULL,
        artist_location varchar(200) NULL,
        artist_name varchar(200) NULL,
        song_id varchar(18) NULL,
        title varchar(200) NULL,
        duration numeric NULL,
        year smallint NULL
    );
""")

# A fact table related to time, user, song, and artist dimensions.
# We use the time dimension for sortkey and song_id as distkey. The distkey
# has been chosen due to the weight of the songs table (up to 1 mio rows) 
# and considering the role that the song entity will play in the analytical queries.
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL UNIQUE sortkey,
        user_id int NOT NULL REFERENCES users(user_id),
        level char(4) NOT NULL,
        song_id varchar(18) NOT NULL REFERENCES songs(song_id) distkey,
        artist_id varchar(18) NOT NULL REFERENCES artists(artist_id),
        session_id int NOT NULL,
        location varchar(50) NOT NULL,
        user_agent varchar(200) NOT NULL
    );    
""")

# A dimension table of users.
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int NOT NULL PRIMARY KEY sortkey,
        first_name varchar(20) NOT NULL,
        last_name varchar(50) NOT NULL,
        gender char(1) NOT NULL,
        level char(4) NOT NULL
    )
    diststyle all;
""")

# A dimension song table with distkey.
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar(18) NOT NULL PRIMARY KEY sortkey distkey,
        title varchar(200) NOT NULL,
        artist_id varchar(18) NOT NULL REFERENCES artists(artist_id),
        year int NOT NULL,
        duration numeric NOT NULL
    );
""")

# A dimension table of artists.
artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar(20) NOT NULL PRIMARY KEY sortkey,
        name varchar(200) NOT NULL,
        location varchar(200) NULL,
        latitude varchar(30) NULL,
        longitude varchar(30) NULL
    )
    diststyle all;    
""")

# A dimension time table.
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp NOT NULL PRIMARY KEY sortkey,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    )
    diststyle all;    
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    CREDENTIALS {}
    REGION 'us-west-2'
    FORMAT AS JSON {};    
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS {}
    REGION 'us-west-2'
    JSON 'auto';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

# The join between the both staging table is not perfect.
# We are forced to use non-identifier text attributes which are prone to duplictates.
# Since the rate of mismatched rows is very high we conclude that poor matching
# is a result of INCOMPLETE staging data.
songplay_table_insert = ("""
    INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + (A.ts/1000)::BIGINT * INTERVAL '1 second'
        , A.userId::INTEGER
        , A.level
        , B.song_id
        , B.artist_id
        , A.sessionId
        , A.location
        , A.userAgent
    FROM staging_events AS A
    INNER JOIN staging_songs AS B
        ON A.song = B.title
        AND A.artist = B.artist_name;
""")

# Since the both keys (userId) and (userId, firstName, lastName, gender)
# have the same aggregation power (confirmed by example) there is no difference in aggregation level
# if we use the composed aggregation key (using DISTINCT instruction). 
# Note that we use the user's "last level" found in the dataset since the level 
# is a user-time attribute.
user_table_insert = ("""
    INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT A.userId::INTEGER, A.firstName, A.lastName, A.gender, A.level
    FROM staging_events AS A
    INNER JOIN (
        SELECT userId, MAX(ts) AS tsLast
        FROM staging_events
        GROUP BY userId
    ) AS B
        ON A.userId = B.userId
        AND A.ts = B.tsLast
    WHERE A.userId IS NOT NULL;
""")

# We opt for inserting ALL staging songs/artists.
# The other option would be to insert only songs/artists from the MATCHING rows.
# Although the uniqueness of the staging table has shown that the song_id column is the 
# unique key we shouldn't trust the staging data we have. Consequently, we aggregate
# songs by song_id to guarantee that the result will hold uniqueness by song_id.
song_table_insert = ("""
    INSERT INTO songs
    (song_id, title, artist_id, year, duration)
    SELECT song_id, MIN(title), MIN(artist_id), MIN(year), MIN(duration)
    FROM staging_songs
    GROUP BY song_id;
""")

# We opt for inserting ALL staging songs/artists.
# The other option would be to insert only songs/artists from the MATCHING rows.
artist_table_insert = ("""
    INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
    SELECT artist_id, MIN(artist_name), MIN(artist_location), MIN(artist_latitude), MIN(artist_longitude)
    FROM staging_songs
    GROUP BY artist_id;
""")

time_table_insert = ("""
    INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    SELECT A.start_time
        , EXTRACT(hour FROM A.start_time)
        , EXTRACT(day FROM A.start_time)
        , EXTRACT(week FROM A.start_time)
        , EXTRACT(month FROM A.start_time)
        , EXTRACT(year FROM A.start_time)
        , EXTRACT(weekday FROM A.start_time)        
    FROM (
        SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000)::BIGINT 
            * INTERVAL '1 second' AS start_time
        FROM staging_events
    ) AS A;
""")

# CHECK QUERIES
table_list_query = ("""
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables 
    WHERE table_schema = 'public';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, time_table_create, artist_table_create, song_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS log_events_staging (
                        artist varchar(500),
	                    auth varchar(25) NOT NULL,
	                    first_name varchar(100), 
	                    gender varchar(10),
	                    item_in_session int,
	                    last_name varchar(100), 
	                    length numeric, 
	                    level varchar(10) ,
	                    location varchar(250), 
	                    method varchar(10),
	                    page varchar(50) ,
	                    registration bigint,
	                    session_id int, 
	                    song varchar(500), 
	                    status int,
	                    ts timestamp,
	                    user_agent varchar(250),
	                    user_id int ) """)

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS songs_staging (
                        num_songs int NOT NULL,
                        artist_id varchar(30) NOT NULL,
                        artist_latitude numeric,
                        artist_longitude numeric,
                        artist_location varchar(500),
                        artist_name varchar(500) NOT NULL,
                        song_id varchar(30),
                        title varchar(500) NOT NULL, 
                        duration numeric NOT NULL,
                        year int) """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id int NOT NULL IDENTITY(0,1), 
                                start_time timestamp NOT NULL distkey, 
                                user_id int NOT NULL sortkey, 
                                level varchar(10), 
                                song_id varchar(30),
                                artist_id varchar(30),
                                session_id int, 
                                location varchar(250),
                                user_agent varchar(250),
                                PRIMARY KEY(songplay_id)) """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users ( 
                            user_id int sortkey, 
                            first_name varchar(100) NOT NULL,
                            last_name varchar(100) NOT NULL, 
                            gender varchar(10) NOT NULL, 
                            level varchar(10) NOT NULL,
                            PRIMARY KEY(user_id)) diststyle all """ )

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (  
                            song_id varchar(30) sortkey, 
                            title varchar(100) NOT NULL,  
                            artist_id varchar(30) NOT NULL, 
                            year int , 
                            duration numeric NOT NULL,
                            PRIMARY KEY(song_id)) diststyle all """ )

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id varchar(30) sortkey, 
                            name varchar(100) NOT NULL, 
                            location varchar(100), 
                            latitude numeric, 
                            longitude numeric, 
                            PRIMARY KEY(artist_id)) diststyle all """ )

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp sortkey distkey, 
                            hour int, 
                            day int,
                            week int, 
                            month text, 
                            year int, 
                            weekday text, 
                            PRIMARY KEY(start_time) )""")

# STAGING TABLES

staging_events_copy = ("""COPY log_events_staging 
                          FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          REGION '{}'
                          JSON '{}'
                          TIMEFORMAT as 'epochmillisecs'
                        """).format(config['S3']['log_data'],
                                    config['IAM_ROLE']['arn'],
                                    config['AWS_CONFIG']['region'],
                                    config['S3']['log_jsonpath'])

staging_songs_copy = ("""COPY songs_staging 
                          FROM '{}' 
                          CREDENTIALS 'aws_iam_role={}'
                          REGION '{}'
                          JSON 'auto'
                        """).format(config['S3']['song_data'],
                                    config['IAM_ROLE']['arn'],
                                    config['AWS_CONFIG']['region'])

# FINAL TABLES

songplay_table_insert = (""" INSERT into songplays (start_time,
                                                    user_id, 
                                                    level, 
                                                    song_id,
                                                    artist_id,
                                                    session_id, 
                                                    location,
                                                    user_agent ) (select  ts, 
                                                            nvl(ls.user_id, -1), 
                                                            ls.level, 
                                                            s.song_id, 
                                                            s.artist_id,
                                                            ls.session_id,
                                                            ls.location,
                                                            ls.user_agent
                                                            from 
                                                                log_events_staging ls
                                                                left outer join
                                                                songs_staging s
                                                                on ls.artist = s.artist_name
                                                                and ls.song = s.title
                                                             )
""")

user_table_insert = (""" INSERT into users (select 
                                                user_id, 
                                                first_name, 
                                                last_name, 
                                                gender, 
                                                level 
                                                from log_events_staging 
                                                where page = 'NextSong' 
                                            )""")

song_table_insert = (""" INSERT into songs (select 
                                                song_id,
                                                title,
                                                artist_id,
                                                year,
                                                duration
                                                from 
                                                songs_staging
                                            )""")

artist_table_insert = (""" INSERT into artists (select 
                                                    artist_id,
                                                    artist_name,
                                                    artist_location,
                                                    artist_latitude,
                                                    artist_longitude
                                                    from 
                                                    songs_staging
                                               )""")

time_table_insert = (""" INSERT into time (select ts,
                                                  to_char(ts, 'HH24') :: int,
                                                  to_char(ts, 'DD') :: int,
                                                  to_char(ts,'WW') :: int,
                                                  to_char(ts,'Mon'),
                                                  to_char(ts, 'YYYY') :: int,
                                                  to_char(ts, 'Day') from log_events_staging)""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

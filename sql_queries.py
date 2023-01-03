import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events
								(
    								artist VARCHAR,
    								auth VARCHAR,
									firstName VARCHAR,
									gender VARCHAR,
									itemInSession INTEGER,
									lastName VARCHAR,
									length FLOAT,
									level VARCHAR,
									location VARCHAR,
									method VARCHAR,
									page VARCHAR,
									registration FLOAT,
									sessionId INTEGER,
									song VARCHAR,
									status INTEGER,
									ts BIGINT,
									userAgent VARCHAR,
									userId VARCHAR
								);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs
								(
									num_songs INTEGER,
									artist_id VARCHAR,
									artist_latitude FLOAT,
									artist_longitude FLOAT,
									artist_location VARCHAR,
									artist_name VARCHAR,
									song_id VARCHAR,
									title VARCHAR,
									duration FLOAT,
									year INTEGER
								);
""")

songplay_table_create = ("""CREATE TABLE songplays
							(
								songplay_id BIGINT IDENTITY(0,1) NOT NULL sortkey,
								start_time TIMESTAMP NOT NULL,
								user_id VARCHAR NOT NULL,
								level VARCHAR NOT NULL,
								song_id VARCHAR NOT NULL distkey,
								artist_id VARCHAR NOT NULL,
								session_id INT NOT NULL,
								location VARCHAR,
								user_agent VARCHAR
							);
""")

user_table_create = ("""CREATE TABLE users
						(
							user_id VARCHAR NOT NULL sortkey,
							first_name VARCHAR,
							last_name VARCHAR,
							gender VARCHAR,
							level VARCHAR
    					);
""")

song_table_create = ("""CREATE TABLE songs
						(
							song_id VARCHAR NOT NULL sortkey,
							title VARCHAR NOT NULL,
							artist_id VARCHAR NOT NULL distkey,
							year INT NOT NULL,
							duration FLOAT NOT NULL
    					);
""")

artist_table_create = ("""CREATE TABLE artists
							(
								artist_id VARCHAR NOT NULL sortkey,
								name VARCHAR NOT NULL,
								location VARCHAR,
								latitude FLOAT,
								longitude FLOAT
    						);
""")

time_table_create = ("""CREATE TABLE time
						(
							start_time TIMESTAMP NOT NULL sortkey,
							hour INT NOT NULL,
							day INT NOT NULL,
							week INT NOT NULL,
							month INT NOT NULL,
							year INT NOT NULL,
							weekday INT NOT NULL
    					);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
							iam_role {}
							format as json {}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs FROM {}
                        iam_role {}
                        format as json 'auto'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
        					SELECT 	timestamp 'epoch' + ts::numeric * interval'0.001 seconds' start_time,
									events.userId user_id,
									events.level,
									songs.song_id,
									songs.artist_id,
									events.sessionId session_id,
									events.location,
									events.userAgent user_agent
				      	  	FROM staging_events events
							JOIN staging_songs songs
					  	 		ON events.song=songs.title
				    		    AND events.length=songs.duration
				        		AND events.artist=songs.artist_name
				        	WHERE events.page='NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id,first_name,last_name,gender,level)
       					SELECT  DISTINCT userId user_id,
				                firstName first_name,
                				lastName last_name,
				                gender,
				                level
                		FROM staging_events
""")

song_table_insert = ("""INSERT INTO songs (song_id,title,artist_id,year,duration)
        				SELECT  DISTINCT song_id,
            				    title,
               					artist_id,
				                year,
				                duration
				                FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id,name,location,latitude,longitude)
        					SELECT 	DISTINCT artist_id,
					                artist_name,
                					artist_location,
      						 		artist_latitude,
					                artist_longitude
			                FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
       					SELECT  distinct(timestamp 'epoch' + ts::numeric * interval'0.001 seconds') AS start_time,
				                extract(hour from start_time) AS hour,
              				 	extract(day from start_time) AS day,
				                extract(week from start_time) AS week,
				                extract(month from start_time) AS month,
				                extract(year from start_time) AS year,
				                extract(dow from start_time) AS weekday
				        FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

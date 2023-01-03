# Udacity Data Engineer nanodegree: Cloud Data Warehouse project

## Context and purpose
Sparkify is a music streaming company that has data about songs and when and how they are played by their users
The aim of this project is to turn these massive datasets into a structure that can easily be used for analytical purposes

## Database design

	'staging_events - contains raw data about events'
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

	'staging_songs - contains raw data about songs'
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
	
	'fact-songplays - contains analytics data about songplays'
		songplay_id BIGINT IDENTITY(0,1) NOT NULL sortkey,
		start_time TIMESTAMP NOT NULL,
		user_id VARCHAR NOT NULL,
		level VARCHAR NOT NULL,
		song_id VARCHAR NOT NULL distkey,
		artist_id VARCHAR NOT NULL,
		session_id INT NOT NULL,
		location VARCHAR,
		user_agent VARCHAR

	'dim-users - contains analytics data about users'
		user_id VARCHAR NOT NULL sortkey,
		first_name VARCHAR,
		last_name VARCHAR,
		gender VARCHAR,
		level VARCHAR

	'dim-songs - contains analytics data about songs'
		song_id VARCHAR NOT NULL sortkey,
		title VARCHAR NOT NULL,
		artist_id VARCHAR NOT NULL distkey,
		year INT NOT NULL,
		duration FLOAT NOT NULL
	
	'dim-artists - contains analytics data about artists'
		artist_id VARCHAR NOT NULL sortkey,
		name VARCHAR NOT NULL,
		location VARCHAR,
		latitude FLOAT,
		longitude FLOAT
	
	'dim-time - contains analytics data bout time'
		start_time TIMESTAMP NOT NULL sortkey,
		hour INT NOT NULL,
		day INT NOT NULL,
		week INT NOT NULL,
		month INT NOT NULL,
		year INT NOT NULL,
		weekday INT NOT NULL
	


## Pipeline design

### Staging
The information about songs and events are loaded into staging tables ('staging_events' and 'staging_songs')

### Star schema
We then populate 5 tables of the selected star schema with information sourced from the staging tables.
We have 4 dimensions tables ('users','songs','artists' and 'time') and 1 fact table ('songplays')

## DB and pipeline usage

### dwh.cfg
This file will contain all required credentials to connect to the cluster as per the following structure
	
	[CLUSTER]
	HOST=
	DB_NAME=
	DB_USER=
	DB_PASSWORD=
	DB_PORT=
	
	[IAM_ROLE]
	ARN=''
	
	[S3]
	LOG_DATA='s3://udacity-dend/log-data'
	LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
	SONG_DATA='s3://udacity-dend/song-data'
	
### create_tables.py
This file must be run prior to running 'etl.py'
It will connect to the redshift cluster (thanks to credentials stored in 'dwh.cfg') and DROP (if necessary) then CREATE the following tables:
staging:
	*staging_events
	*staging_songs

analytics:
	*users
	*songs
	*artists
	*time
	*songplays

### etl.py
'create_tables.py' must be run first!
Executing this file will:
	*load both datasets into the staging tables
	*process the data from the staging tables and load it into the fact and dimension star schema tables

All queries are stored in 'sql_queries.py'

### sql_queries.py
This file is loaded by 'etl.py' and contains all the queries (in SQL) used to carry out (DROP), CREATE and INSERT statements. 

## Sample queries

### Top 3 ever played songs

	'SELECT songs.title,COUNT(*) totalcounter
        FROM songplays
        JOIN songs ON songplays.song_id=songs.song_id
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 3'

| title													| totalcounter	|
| -----------------------------------------------------	| ------------- |
| You're The One										| 37 			|
| I CAN'T GET STARTED									| 9 			|
| Catch You Baby (Steve Pitron & Max Sanna Radio Edit)	| 9			 	|

| First Header  | Second Header |
| ------------- | ------------- |
| Content Cell  | Content Cell  |
| Content Cell  | Content Cell  |

### Top day of the week in terms of song plays
	'SELECT weekday,COUNT(*) AS totalcounter
        FROM songplays
        LEFT JOIN time
        ON songplays.start_time=time.start_time
        GROUP BY 1
        ORDER BY 2 desc'
        
    weekday	totalcounter
	4	63
	1	59
	3	58
	5	51
	2	42
	6	30
	0	16

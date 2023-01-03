import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
	"""Copies the raw data from S3 Bucket into the 2 staging tables
	
	PARAMETERS:
	cur: cursor of the psycopg2 connection to the redshift cluster DB
	conn: psycopg2 connection to the redshift cluster DB
	
	OUTPUT:
	The raw songs and events datasets have been loaded into the staging tables 
	"""
	for query in copy_table_queries:
		print('executing query:')
		print(query[0:100]+'...')
		cur.execute(query)
		conn.commit()
		print('done')
		print('--')


def insert_tables(cur, conn):
	"""Structures the raw data from the staging tables into the 5 analytics tables
		
	PARAMETERS:
	cur: cursor of the psycopg2 connection to the redshift cluster DB
	conn: psycopg2 connection to the redshift cluster DB
	
	OUTPUT:
	The raw songs and events datasets have been structured in the 5 analytics table in the form of a star schema 
	"""
	for query in insert_table_queries:
		cur.execute(query)
		conn.commit()


def main():
	""" CAUTION: to be executed after create_tables.py has been executed
	- Creates a connection to the Redshift cluster DB
	- Loads the rawdata into the staging tables (load_staging_tables function)
	- Processes the rawdata and stores into analytics tabels (insert_tables function)
	"""
	print('loading configuration...')
	config = configparser.ConfigParser()
	config.read('dwh.cfg')
	print('configuration loaded')

	print('connecting to cluster...')
	conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
	cur = conn.cursor()
	print('connection established')

	print('loading information into the staging tables...')
	load_staging_tables(cur, conn)
	print('staging tables loaded')
	
	print('loading information into the analytics tables...')
	insert_tables(cur, conn)
	print('analytics tables loaded')

	print('closing connection...')
	conn.close()
	print('connection closed')


if __name__ == "__main__":
    main()
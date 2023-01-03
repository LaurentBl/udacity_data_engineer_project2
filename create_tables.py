import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

#drops staging and analytics tables
def drop_tables(cur, conn):
	"""Drops all tables as per the queries listed in the drop_table_queries list imported from sql_queries
	
	PARAMETERS:
	cur: cursor of the psycopg2 connection to the redshift cluster DB
	conn: psycopg2 connection to the redshift cluster DB
	
	OUTPUT:
	All queries in the drop_table_queries list have been executed, resulting in the tables being dropped.
	"""
	for query in drop_table_queries:
		cur.execute(query)
		conn.commit()


def create_tables(cur, conn):
	"""Creates all tables as per the queries listed in the create_table_queries list imported from sql_queries
	
	PARAMETERS:
	cur: cursor of the psycopg2 connection to the redshift cluster DB
	conn: psycopg2 connection to the redshift cluster DB
	
	OUTPUT:
	All queries in the create_table_queries list have been executed, resulting in the tables being created.
	"""
	for query in create_table_queries:
		cur.execute(query)
		conn.commit()


def main():
	"""- Establishes a connection to the Redshift cluster DB
	- Drops all tables
	- Creates all tables
	- Closes the connection
	"""
	print('loading configuration...')
	config = configparser.ConfigParser()
	config.read('dwh.cfg')
	print('configuration loaded')

	print('creating conneciton to the Redshift cluster...')
	conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
	cur = conn.cursor()
	print('connection established')

	print('dropping tables...')
	drop_tables(cur, conn)
	print('tables dropped')
	
	print('creating tables...')
	create_tables(cur, conn)
	print('tables created')

	print('closing connection...')
	conn.close()
	print('connection closed')


if __name__ == "__main__":
    main()
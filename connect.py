import psycopg2
import sys
import pprint
from config import config
from queries import *

def main():
	executeQuery(connect(), redVialQuery)

#Connects to database using credentials obtained with config
def connect():
	conn = None
	try:
        # read connection parameters
		params = config()
        # connect to the PostgreSQL server
		print('Connecting to the PostgreSQL database')
		conn = psycopg2.connect(**params)
		# create a cursor
		cursor = conn.cursor()
		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = conn.cursor()
		print('Connected!\n')

	except (Exception, psycopg2.DatabaseError) as error:
		print(error)

	return cursor
	

#Executes a query in a database, query is a string
def executeQuery(cursor, query):
	# execute our Query
	cursor.execute(query)
 
	# retrieve the records from the database
	records = cursor.fetchall()
 
	# print out the records using pretty print
	pprint.pprint(records)
	

if __name__ == "__main__":
	main()


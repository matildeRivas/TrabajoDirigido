import psycopg2
import sys
import pprint
import config
from queries import *

def main():
	executeQuery(connect(sys.argv[1]), pointsQuery)

	
#Connects to given database using credentials in config file
#TODO return cursor
def connect(dbname):
	#Define our connection string
	conn_string = "host='localhost' dbname='" + dbname +"' user='" + config.user + "' password=" + config.password
 
	# print the connection string we will use to connect
	print("Connecting to database\n	->" + conn_string)
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
	print('Connected!\n')

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

import psycopg2
from psycopg2.extensions import AsIs
import sys
import pprint
from config import config
from queries import *

def main(argv):
	if(len(sys.argv) < 4 or len(sys.argv) > 4):
		print("Debe ingresar dos comunas y el mapa de caminos")
	else:
		connect(sys.argv[1], sys.argv[2], sys.argv[3])


#Connects to database using credentials obtained with config
# @params:
# name of starting point
# name of end location
# name of paths map
def connect(origin, end, pathsMap):
	conn = None
	try:
        # read connection parameters
		params = config()
        # connect to the PostgreSQL server
		print('Connecting to the PostgreSQL database')
		conn = psycopg2.connect(**params)
		print('Connected!\n')
		# create a cursor
		cursor = conn.cursor()
		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = conn.cursor()
		# execute queries to find paths
		
		# close communication with the PostgreSQL database server
		cursor.close()
		# commit the changes
		print("commited")
		conn.commit()
		print(timeDelta)
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if conn is not None:
			conn.close()


if __name__ == "__main__":
	main(sys.argv)

import psycopg2
import sys
import pprint
import config
 
def main():
	#Define our connection string
	conn_string = "host='localhost' dbname='trabajodirigido' user='" + config.user + "' password=" + config.password
 
	# print the connection string we will use to connect
	print "Connecting to database\n	->%s" % (conn_string)
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
	print "Connected!\n"
	# execute our Query
	cursor.execute("with line_counts (cts, id) as (select ST_NPoints(geom)-1, id from mapa_cobre), series(num, id) as (select generate_series(1, cts), id from line_counts) select ST_PointN(geom , num) as inicio, ST_PointN(geom, num + 1) as fin, mapa_cobre.id from series inner join mapa_cobre on series.id = mapa_cobre.id; ")
 
	# retrieve the records from the database
	records = cursor.fetchall()
 
	# print out the records using pretty print
	# note that the NAMES of the columns are not shown, instead just indexes.
	# for most people this isn't very useful so we'll show you how to return
	# columns as a dictionary (hash) in the next example.
	pprint.pprint(records)
 
if __name__ == "__main__":
	main()

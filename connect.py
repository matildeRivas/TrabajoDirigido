import psycopg2
from psycopg2.extensions import AsIs
import sys
from config import config
from queries import *
import datetime


def main(argv):
	if (len(sys.argv) < 3 or len(sys.argv) > 3):
		print("Debe ingresar nombre del mapa y el tipo de tramo")
	else:
		connect(sys.argv[1], sys.argv[2])


# Connects to database using credentials obtained with config
# @params:
# name of table containing map, geometry column must de named "geom"
# type of path: nada, postacion, ferry or directo 
def connect(tableToInsersect, pathType):
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
		# execute queries to find intersections
		currentTime = datetime.datetime.now()
		intersectionsQueries(cursor, tableToInsersect, pathType)
		timeDelta = datetime.datetime.now() - currentTime
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


# cost per type
costsDictionary = {
	"nada": 25000,
	"postacion": 17000
}
costsDictionary["ferry"] = costsDictionary["nada"] * 2
costsDictionary["fibra"] = costsDictionary["nada"] * 2


# turns table with segments into a dictionary, adding information needed later
# @params:
# PSQL table with a segment's start and endpoint coordinates, id, and the id of the original segment
def processPoints(points):
	data = {}
	points = sorted(points, key=lambda x: x[1])
	for i in points:
		a = i[3] - i[1]  # B.y - A.y
		b = i[0] - i[2]  # A.x - B.x
		c = a * i[0] + b * i[1]  # a*A.x + b*A.y
		segment = {}
		segment["origin_id"] = i[4]
		segment["id"] = i[5]
		segment["x1"] = i[0]
		segment["y1"] = i[1]
		segment["x2"] = i[2]
		segment["y2"] = i[3]
		segment["A"] = a
		segment["B"] = b
		segment["C"] = c
		data[i[5]] = segment
	return data


# Determines whether a number is between other two
# @params:
# number in question
# boundaries
def within(p, q, r):
	return (p >= q and p <= r) or ((p <= q and p >= r))


# Finds all intersections in a map
def findIntersections(segments, cursor, tableName, filteredTableName):
	# TODO: optimize
	for (L1, v1) in segments.items():
		for (L2, v2) in segments.items():
			if (v1["id"] > v2["id"]):
				D = v1["A"] * v2["B"] - v1["B"] * v2["A"]
				# hay interseccion
				if D != 0:
					Dx = v1["C"] * v2["B"] - v1["B"] * v2["C"]
					Dy = v1["A"] * v2["C"] - v1["C"] * v2["A"]
					x = Dx / D
					y = Dy / D
					if within(x, v1["x1"], v1["x2"]) or within(x, v2["x1"], v2["x2"]) and within(y, v1["y1"], v1["y2"]) or within(
							y, v2["y1"], v2["y2"]):
						cursor.execute("with line1 (geom, origin_id, id) as (select ST_MakeLine(ST_MakePoint(%s, %s), ST_MakePoint(%s, %s)), %s, %s ) ,\
						line2 (geom, origin_id, id) as (select ST_MakeLine(ST_MakePoint(%s, %s), ST_MakePoint(%s, %s)), %s, %s )\
						insert into %s select ST_MakePoint(ST_X(ST_intersection(a.geom, b.geom))::NUMERIC(8,5), ST_Y(ST_intersection(a.geom, b.geom))::NUMERIC(8,5)), a.origin_id, b.origin_id from line1 a, line2 b WHERE a.id=%s and b.id=%s AND ST_Intersects(a.geom,b.geom)\
						and not ST_Equals(ST_EndPoint(a.geom), ST_StartPoint(b.geom)) and not ST_Equals(ST_EndPoint(b.geom), ST_StartPoint(a.geom));", \
													 (v1["x1"], v1["y1"], v1["x2"], v1["y2"], v1["origin_id"], v1["id"], \
														v2["x1"], v2["y1"], v2["x2"], v2["y2"], v2["origin_id"], v2["id"] \
															, AsIs(tableName), v1["id"], v2["id"]))
				# no hay interseccion
				else:
					pass


# Creates a map with all paths 
def intersectionsQueries(cursor, mapName, pathType):
	truncatedTableName = "truncated_" + mapName
	cursor.execute("create table %s as select * from %s;", (AsIs(truncatedTableName), AsIs(mapName)))
	cursor.execute("update %s set geom=st_snapToGrid(geom, 0.00001);", (AsIs(truncatedTableName),))
	# Convert map to points, save it into table called "mapName_points"
	# create new table "mapName_points"
	pointsTableName = mapName + "_points"
	cursor.execute("create table %s (x1 float, y1 float, x2 float, y2 float, origin_id int);", (AsIs(pointsTableName),))
	print(cursor.statusmessage)
	# get all points truncated to 5 decimals
	cursor.execute("with line_counts (cts, id) as (select ST_NPoints(geom) - 1, id from %s), series(num, id) as " \
								 + "(select generate_series(1, cts), id from line_counts) insert into %s select distinct " \
								 + " ST_X(ST_PointN(geom, num)) as x1, ST_Y(ST_PointN(geom, num)) as y1, " \
								 + " ST_X(ST_PointN(geom, num+1))as x2, ST_Y(ST_PointN(geom, num+1)) as y2," \
								 + " %s.id as origin_id from series inner join %s on series.id = %s.id;",
								 (AsIs(truncatedTableName), AsIs(pointsTableName), AsIs(truncatedTableName), AsIs(truncatedTableName),
									AsIs(truncatedTableName)))
	# filter duplicates and save result in table called "filtered_newTableName"
	print(cursor.statusmessage)
	filteredTableName = "filtered_" + pointsTableName
	cursor.execute("create table %s as select distinct on (x1, y1, x2, y2) * from %s;",
								 (AsIs(filteredTableName), AsIs(pointsTableName)))
	# add index to table
	print(cursor.statusmessage)
	cursor.execute("alter table %s add id int generated by default as identity primary key;", (AsIs(filteredTableName),))
	# create intersections table
	intersectionsName = mapName + "_intersections"
	cursor.execute(
		"create table %s (geom geometry, origin_a int, origin_b int, id int generated by default as identity primary key);",
		(AsIs(intersectionsName),))
	cursor.execute("select * from %s;", (AsIs(filteredTableName),))
	print(cursor.statusmessage)
	points = cursor.fetchall()
	pointsDict = processPoints(points)
	# find intersections
	print("finding intersections")
	findIntersections(pointsDict, cursor, intersectionsName, filteredTableName)
	print("found intersections")
	index = mapName + "geomIndex"
	# create paths table
	pathsTableName = "caminos_" + mapName
	cursor.execute(
		"create table %s (camino geometry, x1 float, y1 float, x2 float, y2 float, tipo varchar(255), id_origen int, largo float, costo float, tabla_origen varchar(255), origen geometry, fin geometry, id int generated by default as identity primary key) ;",
		(AsIs(pathsTableName),))
	# set an index on the new table
	cursor.execute("update %s set geom = ST_SetSRID(geom, 4326);", (AsIs(intersectionsName),))
	cursor.execute("update %s set geom = ST_snaptogrid(geom, 0.00001);", (AsIs(intersectionsName),))
	cursor.execute("create index %s on %s using GIST (geom);  analyze %s;",
								 (AsIs(index), AsIs(intersectionsName), AsIs(intersectionsName)))
	# split lines according to intersections, inserting both halves in paths table
	cursor.execute("with a (geomCol, id) as (select ST_Split(ST_snap(m.geom, i.geom, 0.00001), i.geom), origin_a from %s m, %s i where m.id=i.origin_a and m.id not in (select id_origen from %s)),\
	 series (num) as (select generate_series(1, 2)) \
	insert into %s (camino, x1, y1, x2, y2, tipo, id_origen, origen, fin) \
	select ST_GeometryN(a.geomCol, num), ST_X(ST_StartPoint(ST_GeometryN(a.geomCol, num))), ST_Y(ST_StartPoint(ST_GeometryN(a.geomCol, num))),\
	ST_X(ST_EndPoint(ST_GeometryN(a.geomCol, num))), ST_Y(ST_EndPoint(ST_GeometryN(a.geomCol, num))),%s, a.id , ST_StartPoint(ST_GeometryN(a.geomCol, num)), \
	ST_EndPoint(ST_GeometryN(a.geomCol, num))  from a, series;",
								 (AsIs(truncatedTableName), AsIs(intersectionsName), AsIs(pathsTableName), AsIs(pathsTableName),
									pathType))

	cursor.execute("with a (geomCol, id) as (select ST_Split(ST_snap(m.geom, i.geom, 0.00001), i.geom), origin_b from %s m, %s i where m.id=i.origin_b and m.id not in (select id_origen from %s) ),\
	series (num) as (select generate_series(1, 2)) \
	insert into %s (camino, x1, y1, x2, y2, tipo, id_origen, origen, fin) \
	select ST_GeometryN(a.geomCol, num), ST_X(ST_StartPoint(ST_GeometryN(a.geomCol, num))), ST_Y(ST_StartPoint(ST_GeometryN(a.geomCol, num))),\
	ST_X(ST_EndPoint(ST_GeometryN(a.geomCol, num))), ST_Y(ST_EndPoint(ST_GeometryN(a.geomCol, num))),%s, a.id , ST_StartPoint(ST_GeometryN(a.geomCol, num)), \
	ST_EndPoint(ST_GeometryN(a.geomCol, num))  from a, series;",
								 (AsIs(mapName), AsIs(intersectionsName), AsIs(pathsTableName), AsIs(pathsTableName), pathType))
	# insert rest of segments
	cursor.execute("insert into %s (camino, x1, y1, x2, y2, tipo, id_origen, origen, fin) select geom, ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), \
	ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)), %s, id, ST_StartPoint(geom), ST_EndPoint(geom) from %s where id not in (select id_origen from %s)",
								 (AsIs(pathsTableName), pathType, AsIs(truncatedTableName), AsIs(pathsTableName)))
	pathIndex = pathsTableName + "GeomIndex"
	cursor.execute("create index %s on %s using GIST (camino);  analyze %s;",
								 (AsIs(pathIndex), AsIs(pathsTableName), AsIs(pathsTableName)))
	# update paths table with additional information: tabla_origen, haversine distance (in meters) and total cost
	cursor.execute(
		"delete from %s a where a.camino in (select p.camino from %s p, %s b where ST_covers(ST_snap(p.camino, b.camino, 0.00001), b.camino) and b.id!=p.id) ;",
		(AsIs(pathsTableName), AsIs(pathsTableName), AsIs(pathsTableName)))
	costo = costsDictionary[pathType]
	cursor.execute("with line_counts (cts, id) as (select ST_NPoints(camino) - 1, id from %s),\
	 series(num, id) as (select generate_series(1, cts), id from line_counts),\
	 dist(d, id) as (select sum(ST_DistanceSphere(ST_PointN(camino, num), ST_PointN(camino, num+1))), m.id from series inner join %s m on series.id = m.id group by m.id) \
	 update %s mapa set tabla_origen = %s, largo = dist.d from dist where mapa.id=dist.id ;",
								 (AsIs(pathsTableName), AsIs(pathsTableName), AsIs(pathsTableName), mapName))
	cursor.execute("update %s set costo = largo/1000 * %s;", (AsIs(pathsTableName), costo))

	cursor.execute("drop table %s; drop table %s; drop table %s;",
								 (AsIs(pointsTableName), AsIs(filteredTableName), AsIs(truncatedTableName)))


if __name__ == "__main__":
	main(sys.argv)

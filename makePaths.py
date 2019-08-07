import psycopg2
from psycopg2.extensions import AsIs
import sys
from config import config
import datetime


def main(argv):
	if (len(sys.argv) < 2 or len(sys.argv) > 2):
		print("Debe ingresar nombre del mapa ")
	else:
		connect(sys.argv[1])


# Connects to database using credentials obtained with config
# @params:
# name of table containing map, geometry column must de named "geom"
# type of path: nada, postacion, ferry or directo 
def connect(tableToInsersect):
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
		intersectionsQueries(cursor, tableToInsersect)
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
	"postacion": 17000,
	"fibra": 0
}
costsDictionary["ferry"] = costsDictionary["nada"] * 2


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
	return (p >= q and p <= r) or (p <= q and p >= r)


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
def intersectionsQueries(cursor, mapName):
	# creates table with costs
	cursor.execute("create table if not exists costs_table (type varchar, cost_per_km int);")
	cursor.execute(
		"insert into costs_table values ('nada', 25000), ('postacion', 17000), ('fibra', 0), ('ferry', 35000);")
	# truncates points in map
	truncatedTableName = "truncated_" + mapName
	cursor.execute("create table %s as select * from %s;", (AsIs(truncatedTableName), AsIs(mapName)))
	cursor.execute("update %s set geom=st_snapToGrid(geom, 0.00001);", (AsIs(truncatedTableName),))
	# convert map to points, save it into table called "mapName_points"
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
		"create table %s (camino geometry, x1 float, y1 float, x2 float, y2 float, tipo varchar(255), id_origen int, largo float, costo bigint, tabla_origen varchar(255), origen geometry, fin geometry, id int generated by default as identity primary key) ;",
		(AsIs(pathsTableName),))
	# set an index on the new table
	cursor.execute("update %s set geom = ST_SetSRID(geom, 4326);", (AsIs(intersectionsName),))
	cursor.execute("update %s set geom = ST_snaptogrid(geom, 0.00001);", (AsIs(intersectionsName),))
	cursor.execute("create index %s on %s using GIST (geom);  analyze %s;",
								 (AsIs(index), AsIs(intersectionsName), AsIs(intersectionsName)))

	# split lines according to intersections, inserting segments in paths table
	'''cursor.execute("with a (geomCol, id, tipo) as (select ST_Split(ST_snap(m.geom, i.geom, 0.00001), i.geom), origin_a, tipo "
								 "from %s m, %s i where m.id=i.origin_a and m.id not in (select id_origen from %s)),\
	 series (num) as (select generate_series(1, 2)) \
	insert into %s (camino, x1, y1, x2, y2, tipo, id_origen, origen, fin) \
	select ST_snaptogrid(ST_GeometryN(a.geomCol, num), 0.00001), \
	ST_X(ST_StartPoint(ST_GeometryN(a.geomCol, num))), \
	ST_Y(ST_StartPoint(ST_GeometryN(a.geomCol, num))),\
	ST_X(ST_EndPoint(ST_GeometryN(a.geomCol, num))), \
	ST_Y(ST_EndPoint(ST_GeometryN(a.geomCol, num))), \
	a.tipo, a.id , ST_snaptogrid(ST_StartPoint(ST_GeometryN(a.geomCol, num)), 0.00001), \
	ST_snaptogrid(ST_EndPoint(ST_GeometryN(a.geomCol, num)), 0.00001)  from a, series;",
								 (AsIs(truncatedTableName), AsIs(intersectionsName), AsIs(pathsTableName), AsIs(pathsTableName)))

	cursor.execute("with a (geomCol, id, tipo) as (select ST_Split(ST_snap(m.geom, i.geom, 0.00001), i.geom), origin_b, tipo\
	 from %s m, %s i where m.id=i.origin_b and m.id not in (select id_origen from %s) ),\
	series (num) as (select generate_series(1, 2)) \
	insert into %s (camino, x1, y1, x2, y2, tipo, id_origen, origen, fin) \
	select ST_snaptogrid(ST_GeometryN(a.geomCol, num), 0.00001), ST_X(ST_StartPoint(ST_GeometryN(a.geomCol, num))), \
	ST_Y(ST_StartPoint(ST_GeometryN(a.geomCol, num))),\
	ST_X(ST_EndPoint(ST_GeometryN(a.geomCol, num))), ST_Y(ST_EndPoint(ST_GeometryN(a.geomCol, num))), a.tipo, a.id , \
	ST_snaptogrid(ST_StartPoint(ST_GeometryN(a.geomCol, num)), 0.00001), \
	ST_snaptogrid(ST_EndPoint(ST_GeometryN(a.geomCol, num)), 0.00001)  from a, series;",
								 (AsIs(mapName), AsIs(intersectionsName), AsIs(pathsTableName), AsIs(pathsTableName)))
	# insert rest of segments
	cursor.execute("insert into %s (camino, x1, y1, x2, y2, tipo, id_origen, origen, fin) select ST_snaptogrid(geom, 0.00001),\
	 ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)), \
	ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom)), tipo, id, ST_snaptogrid(ST_StartPoint(geom), 0.00001), ST_snaptogrid(ST_EndPoint(geom), 0.00001)\
	 from %s where id not in (select id_origen from %s)",
								 (AsIs(pathsTableName), AsIs(truncatedTableName), AsIs(pathsTableName)))
'''
	cursor.execute(
		"CREATE TABLE points_per_line AS \
		 SELECT a.origin_a AS line_id, ST_Multi(ST_Union(geom)) as geom \
		 FROM %s a GROUP BY a.origin_a \
		 union \
		 SELECT a.origin_b AS line_id, ST_Multi(ST_Union(geom)) as geom \
		 FROM %s a GROUP BY a.origin_b;", (AsIs(intersectionsName), AsIs(intersectionsName)))

	cursor.execute(
		"INSERT INTO %s (camino, tipo, id_origen) \
		 SELECT (ST_Dump(st_split(ST_Snap(a.geom, c.geom, 0.00001),c.geom))).geom AS geom, tipo, line_id \
		FROM %s a JOIN \
		(SELECT line_id, ST_Multi(ST_Union(geom)) AS geom FROM points_per_line GROUP BY line_id) c \
		ON a.id = c.line_id;", (AsIs(pathsTableName), AsIs(truncatedTableName)))

	# insert rest of segments
	cursor.execute("INSERT INTO %s (camino, tipo, id_origen) \
	SELECT ST_snaptogrid(geom, 0.00001), tipo, id \
	FROM %s WHERE id NOT IN (SELECT id_origen FROM %s)",
								 (AsIs(pathsTableName), AsIs(truncatedTableName), AsIs(pathsTableName)))

	pathIndex = pathsTableName + "GeomIndex"
	cursor.execute("CREATE INDEX %s ON %s USING GIST (camino);  ANALYZE %s;",
								 (AsIs(pathIndex), AsIs(pathsTableName), AsIs(pathsTableName)))
	'''cursor.execute(
		"delete from %s a where a.camino in (select b.camino from %s p, %s b where (ST_covers(ST_snap(p.camino, b.camino, 0.00001), \
	b.camino) and not ST_equals(p.camino, b.camino)) and b.id != p.id ) or a.camino = null ;",
		(AsIs(pathsTableName), AsIs(pathsTableName), AsIs(pathsTableName)))
	'''
	cursor.execute("DELETE FROM %s a USING %s b WHERE a.id < b.id AND st_equals(a.camino, b.camino);",
								 (AsIs(pathsTableName), AsIs(pathsTableName)))
	# add "comunas" as vertex
	cursor.execute(
		"create table split_paths(paths , id , tipo , id_origen ) as (select ST_Split(ST_snap(c.camino, p.pun_geom, 0.00001), p.pun_geom), id, tipo, id_origen \
			from %s c, %s p \
			where ST_Intersects(pun_geom, camino)); \
		with series(num) as (select generate_series(1, 2)) \
		 	insert into %s (camino, x1, y1, x2, y2, tipo, id_origen, origen, fin) \
		 	select ST_snaptogrid(ST_geometryN(split_paths.paths, num), 0.00001), \
		 	ST_X(ST_StartPoint(ST_geometryN(split_paths.paths, num))), \
		 	ST_Y(ST_StartPoint(ST_geometryN(split_paths.paths, num))), \
			ST_X(ST_EndPoint(ST_geometryN(split_paths.paths, num))), \
			ST_Y(ST_EndPoint(ST_geometryN(split_paths.paths, num))), \
			split_paths.tipo, split_paths.id_origen, \
			ST_StartPoint(ST_geometryN(split_paths.paths, num)) , \
			ST_EndPoint(ST_geometryN(split_paths.paths, num)) \
			from split_paths, series ;\
		delete from %s where id in (select id from split_paths) ; \
		drop table split_paths;",
		(AsIs(pathsTableName), AsIs("puntos_a_conectar"), AsIs(pathsTableName), AsIs(pathsTableName))
	)
	'''
	a.camino in (select b.camino from % s p, % s b where ST_covers(ST_snap(p.camino, b.camino, 0.00001), \
	b.camino) and b.id != p.id) or'''

	# update paths table with additional information: tabla_origen, haversine distance (in meters) and total cost
	cursor.execute(
		"WITH line_counts (cts, id) AS (SELECT ST_NPoints(camino) - 1, id FROM %s),\
	 series(num, id) AS (SELECT generate_series(1, cts), id FROM line_counts),\
	 dist(d, id) AS (SELECT sum(ST_DistanceSphere(ST_PointN(camino, num), ST_PointN(camino, num+1))), m.id \
	 FROM series inner join %s m ON series.id = m.id GROUP BY m.id) \
	 UPDATE %s mapa SET tabla_origen = %s, x1=ST_X(ST_StartPoint(camino)), y1=ST_Y(ST_StartPoint(camino)), \
	 x2=ST_X(ST_EndPoint(camino)), y2 = ST_Y(ST_EndPoint(camino)), origen = ST_snaptogrid(ST_StartPoint(camino), 0.00001), \
	 fin = ST_snaptogrid(ST_EndPoint(camino), 0.00001),\
	 largo = dist.d FROM dist WHERE mapa.id=dist.id ;",
		(AsIs(pathsTableName), AsIs(pathsTableName), AsIs(pathsTableName), mapName))
	cursor.execute(
		"with c(tipo, cost_per_km) as (select * from costs_table) update %s m set costo = largo/1000 * c.cost_per_km from c where m.tipo=c.tipo;",
		(AsIs(pathsTableName),))

	cursor.execute("drop table %s; drop table %s; drop table %s; drop table points_per_line;",
								 (AsIs(pointsTableName), AsIs(filteredTableName), AsIs(truncatedTableName)))


if __name__ == "__main__":
	main(sys.argv)

from igraph import *
import psycopg2
from psycopg2.extensions import AsIs
import sys
import pprint
from config import config

params = config()
# connect to the PostgreSQL server
print('Connecting to the PostgreSQL database')
conn = psycopg2.connect(**params)
print('Connected!\n')
# create a cursor
cursor = conn.cursor()
# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()
cursor.execute("select origen, fin from caminos_mapa_foa  where camino notnull;")
a = cursor.fetchall()
conn.close()
g = read("foa.ncol", format="ncol", directed=False)
layout = g.layout("kk")
#g.vs["label"] = g.vs["name"]
plot(g, layout=layout)


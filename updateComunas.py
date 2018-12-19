import psycopg2
from psycopg2.extensions import AsIs
import sys
import pprint
from config import config
from queries import *

def main(argv):
	cursor = argv[1]
    mapName = argv[2]
    comunasName = argv[3]
    # add or update column with starting comuna 
    cursor.execute("\
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s AND COLUMN_NAME = %s \
    BEGIN \
     ALTER TABLE %s ADD comuna_inicio int \
    END;", (AsIs(mapName), "comuna_inicio", AsIs(mapName)))
    cursor.execute("WITH com(com_id, m_id) AS (SELECT c.com_id, m.id FROM %s c, %s m WHERE ST_coveredBy(m.origen, c.geom))\
     UPDATE %s SET %s = (SELECT com.com_id FROM com WHERE com.m_id = id) ;", (AsIs(mapName), "comuna_inicio" ,AsIs(comunasName), AsIs(mapName)))
        # add or update column with end comuna 
    cursor.execute("\
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = %s AND COLUMN_NAME = %s \
    BEGIN \
     ALTER TABLE %s ADD comuna_inicio int \
    END;", (AsIs(mapName), "comuna_fin", AsIs(mapName)))
    cursor.execute("WITH com(com_id, m_id) AS (SELECT c.com_id, m.id FROM %s c, %s m WHERE ST_coveredBy(m.fin, c.geom))\
     UPDATE %s SET %s = (SELECT com.com_id FROM com WHERE com.m_id = id) ;", (AsIs(mapName), "comuna_fin" ,AsIs(comunasName), AsIs(mapName)))
    
)


if __name__ == "__main__":
	main(sys.argv)

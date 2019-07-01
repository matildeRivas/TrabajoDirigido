# Internet Resilience
##make_paths
Dado un mapa (POSTGIS) con geometría linestring, encuentra intersecciones entre las geometrías y crea un nuevo mapa con los caminos entre estas.
La nueva tabla tiene los siguientes atributos:
`camino` linestring del camino,
`x1`longitud de punto inicial de camino,
`y1` latitud de punto inicial de camino,
`x2` longitud de punto final de camino,
`y2` latitud de punto final de camino,
`tipo` tipo de camino,
`id_origen` id del linestring al que pertenecía este trozo en la tabla inicial,
`largo` largo del camino,
`tabla_origen` nombre de la tabla ingresada como parámetro,
`origen` geometría del punto inicial del camino,
`fin` geometría del punto final del camino,
`id` identificador del camino


### Configuración y ejecución
En el archivo _database.ini_ se debe colocar la información de la base de datos, de la siguiente foma:
```
[postgresql]
host=<host>
database=<nombre de base de datos>
user=<usuario de postgres>
password=<contraseña>
```
Se pueden cambiar los tipos de caminos y su costo asociado modificando `costsDictionary`, donde el tipo es la llave y el costo por kilómetro el valor.

Para ejecutar, correr ``` python3 intersections.py <nombre mapa> <tipo de camino> ``` en consola

### Requisitos previos
* Python 3.5 o mayor
* Base de datos PSQL extendida con PostGIS
* Mapa a procesar debe tener columna `geom` que contenga geometrías tipo linestring y un identificador en la columna `id `
* La base de datos debe permitir la creación de las tablas "<mapa>\_points", "filtered_<mapa>_points", "<mapa\_intersections", "caminos\_<mapa>". Las primeras dos tablas serán eliminadas al terminar el programa.

##Suurballe
Dado un mapa generado por makePaths y una tabla de puntos a conectar encuentra los segmentos del mapa que deben usarse para otorgarle doble conectividad a los puntos entregados. Los puntos pertenecientes a la tabla de puntos limítrofes solo tndrán una conexión y los correpsondientes a capitales regionales tendrán una tercera conexión a la comuna más cercana. 
En caso de no haber una conexión posible, se crea un nuevo camino hacia la comuna más cercana. 

Los segmentos que deben construirse (caminos usados cuyo osto > 0) se guardarán en una tabla llamada <nombre_mapa>\_construccion, y los ids de todos los caminos que deben usarse se guardan en <nombre_mapa>\_suurballe. Es importante que estas tablas no existan al momento de correr el programa. 

Se retorna el costo total de construcción, para obtenerlo en cualquier momento se debe sumar la columna de costo de <nombre_mapa>\_construccion.




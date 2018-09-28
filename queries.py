 
segmentizeQuery = "with line_counts (cts, id) as (select ST_NPoints(geom) -1, id from mapa_red_vial_2016), series(num, id) as (select generate_series(1, cts), id from line_counts) select ST_MakeLine(ST_PointN(geom, num), ST_PointN(geom, num+1)) as geom, mapa_red_vial_2016.id from series inner join mapa_red_vial_2016 on series.id = mapa_red_vial_2016.id;"

pointsQuery = "with line_counts (cts, id) as (select ST_NPoints(geom)-1, id from mapa_cobre), series(num, id) as (select generate_series(1, cts), id from line_counts) select ST_PointN(geom , num) as inicio, ST_PointN(geom, num + 1) as fin, mapa_cobre.id from series inner join mapa_cobre on series.id = mapa_cobre.id;"

redVialQuery = "select * from mapa_red_vial_2016;"

intersectionQuery = 'insert into intersections select (ST_intersection(a.geom, b.geom)) from mapa_red_vial_2016 a, mapa_red_vial_2016 b where ST_Intersects (a.geom,b.geom) AND a.id<b.id;'

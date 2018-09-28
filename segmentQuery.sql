with line_counts (cts, id) as 
	(select ST_NPoints(geom) -1 , id from mapa_cobre),
	series(num, id) as (select generate_series(1, cts), id from line_counts)
	select ST_MakeLine(ST_PointN(geom, num), ST_PointN(geom, num + 1)) as geom, mapa_cobre.id
	from series inner join mapa_cobre on series.id = mapa_cobre.id;



with                                
  line_counts (cts, id) as
   (select ST_NPoints(geom)-1, id from mapa_cobre),
  series(num, id) as
   (select generate_series(1, cts), id from line_counts)
  insert into prueba select ST_PointN(geom , num) as inicio,
   ST_PointN(geom, num + 1) as fin, mapa_cobre.id from series
 inner join mapa_cobre on series.id = mapa_cobre.id;

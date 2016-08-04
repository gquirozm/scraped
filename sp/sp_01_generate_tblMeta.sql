drop table despegar_meta;
drop table price_meta;
drop table booking_meta;

create table despegar_meta AS(select 
	 a.id
	,a.city
	,a.name
	,expReplace(a.address,'Ubicación: ','') as address
	,a.link
	,a.latitud
	,a.longitud
	,a.source
	,case when aa.mapeo is null then 'false'
	      else aa.mapeo
	 end
	,expHash(a.name || expReplace(a.address,'Ubicación: ','')) as hash			
	,aa.clav_hotel
	,aa.nombre_hotel
	,aa.clav_hotelportal
	,aa.date
	from despegar a LEFT JOIN despegar_mapeo aa ON (a.id = aa.id));


create table price_meta AS(select 
	 a.id
	,a.city
	,a.name
	,a.address
	,a.link
	,a.latitud
	,a.longitud
	,a.source
	,case when aa.mapeo is null then 'false'
	      else aa.mapeo
	 end
	,expHash(a.name || a.address) as hash			
	,aa.clav_hotel
	,aa.nombre_hotel
	,aa.clav_hotelportal
	,aa.date
	from price a LEFT JOIN price_mapeo aa ON (a.id = aa.id));


create table booking_meta AS(select 
	 a.id
	,a.city
	,a.name
	,a.address
	,a.link
	,a.latitud
	,a.longitud
	,a.source
	,case when aa.mapeo is null then 'false'
	      else aa.mapeo
	 end
	,expHash(a.name || a.address) as hash			
	,aa.clav_hotel
	,aa.nombre_hotel
	,aa.clav_hotelportal
	,aa.date
	from booking a LEFT JOIN booking_mapeo aa ON (a.id = aa.id));

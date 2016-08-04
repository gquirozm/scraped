--Procesamos Similarity mas metadatos para BOOKING
insert into full_cluster_tmp01 select 
	a.id,
	a.city,
	a.name,
	a.address,
	a.zipcode, 
	b.city, 
	b.name, 
	b.address,
	metaphone(CAST(a.name AS text),8),
	metaphone(CAST(a.address AS text),12), 
	similarity(CAST(a.name AS text), CAST(b.name AS text)), 
	similarity(CAST(a.address AS text), CAST(b.address AS text)),
	b.hash,
	b.source,
	b.date 
from 
 bestday2 a, booking_meta b
WHERE 
 levenshtein(CAST(a.name AS text), CAST(b.name AS text)) < 5 
ORDER BY 
 metaphone(CAST(a.name AS text),8), metaphone(CAST(b.name AS text),8) ASC;


--Procesamos Similarity mas metadatos para PRICE
insert into full_cluster_tmp01 select 
	a.id,
	a.city,
	a.name,
	a.address,
	a.zipcode, 
	b.city, 
	b.name, 
	b.address,
	metaphone(CAST(a.name AS text),8), 
	metaphone(CAST(a.address AS text),12), 
	similarity(CAST(a.name AS text), CAST(b.name AS text)), 
	similarity(CAST(a.address AS text), CAST(b.address AS text)),
	b.hash,
	b.source,
	b.date 
from 
 bestday2 a, price_meta b
WHERE 
 levenshtein(CAST(a.name AS text), CAST(b.name AS text)) < 5 
ORDER BY 
 metaphone(CAST(a.name AS text),8), metaphone(CAST(b.name AS text),8) ASC;

--Procesamos Similarity mas metadatos para DESPEGAR
insert into full_cluster_tmp01 select 
	a.id,
	a.city,
	a.name,
	a.address,
	a.zipcode, 
	b.city, 
	b.name, 
	b.address,
	metaphone(CAST(a.name AS text),8),
	metaphone(CAST(a.address AS text),12),  
	similarity(CAST(a.name AS text), CAST(b.name AS text)), 
	similarity(CAST(a.address AS text), CAST(b.address AS text)),
	b.hash,
	b.source,
	b.date 
from 
 bestday2 a, despegar_meta b
WHERE 
 levenshtein(CAST(a.name AS text), CAST(b.name AS text)) < 5 
ORDER BY 
 metaphone(CAST(a.name AS text),8), metaphone(CAST(b.name AS text),8) ASC;


--#################################################
--#################################################
--Procesamos el resultado del Similarity mas el universo completo para Booking
insert into full_cluster select distinct
	 a.id
	,aa.m_city
	,aa.m_name
	,aa.m_address
	,a.mapeo
	,a.hash			
	,aa.mtph_name_cluster
	,aa.mtph_addr_cluster
	,aa.d_city
	,aa.d_name
	,aa.d_address	
	,aa.smlty_name_cluster
	,aa.smlty_addr_cluster
	,a.source
	,a.date
	from booking_meta a LEFT JOIN full_cluster_tmp01 aa ON (a.hash = aa.hash and a.source = aa.source_id);

--Procesamos el resultado del Similarity mas el universo completo para PriceTravel
insert into full_cluster select distinct
	 a.id
	,aa.m_city
	,aa.m_name
	,aa.m_address
	,a.mapeo
	,a.hash			
	,aa.mtph_name_cluster
	,aa.mtph_addr_cluster
	,aa.d_city
	,aa.d_name
	,aa.d_address	
	,aa.smlty_name_cluster
	,aa.smlty_addr_cluster
	,a.source
	,a.date
	from price_meta a LEFT JOIN full_cluster_tmp01 aa ON (a.hash = aa.hash and a.source = aa.source_id);

--Procesamos el resultado del Similarity mas el universo completo para Despegar
insert into full_cluster select distinct 
	 a.id
	,aa.m_city
	,aa.m_name
	,aa.m_address
	,a.mapeo
	,a.hash			
	,aa.mtph_name_cluster
	,aa.mtph_addr_cluster
	,aa.d_city
	,aa.d_name
	,aa.d_address	
	,aa.smlty_name_cluster
	,aa.smlty_addr_cluster
	,a.source
	,a.date
	from despegar_meta a LEFT JOIN full_cluster_tmp01 aa ON (a.hash = aa.hash and a.source = aa.source_id);

-- Ponemos la información complementando datos faltantes mas DELTAS

insert into full_cluster_delta select distinct
	 a.id
	,case when b.city is null then 'NO MATCH'
	      else 'MATCH'
	 end
	,b.city
	,b.name
	,b.address
	,a.link
	,a.latitud
	,a.longitud
	,a.mapeo
	,a.hash			
	,b.mtph_name_cluster
	,b.mtph_addr_cluster
	,a.city
	,a.name
	,a.address	
	,b.smlty_name_cluster
	,b.smlty_addr_cluster
	,a.source
	,a.date	
	from despegar_meta a LEFT JOIN full_cluster b ON (a.hash = b.hash and a.source = b.source_id);

insert into full_cluster_delta select distinct
	 a.id
	,case when b.city is null then 'NO MATCH'
	      else 'MATCH'
	 end
	,b.city
	,b.name
	,b.address
	,a.link
	,a.latitud
	,a.longitud
	,a.mapeo
	,a.hash			
	,b.mtph_name_cluster
	,b.mtph_addr_cluster
	,a.city
	,a.name
	,a.address	
	,b.smlty_name_cluster
	,b.smlty_addr_cluster
	,a.source
	,a.date
	from price_meta a LEFT JOIN full_cluster b ON (a.hash = b.hash and a.source = b.source_id);


insert into full_cluster_delta select distinct
	 a.id
	,case when b.city is null then 'NO MATCH'
	      else 'MATCH'
	 end
	,b.city
	,b.name
	,b.address
	,a.link
	,a.latitud
	,a.longitud
	,a.mapeo
	,a.hash			
	,b.mtph_name_cluster
	,b.mtph_addr_cluster
	,a.city
	,a.name
	,a.address	
	,b.smlty_name_cluster
	,b.smlty_addr_cluster
	,a.source
	,a.date
	from booking_meta a LEFT JOIN full_cluster b ON (a.hash = b.hash and a.source = b.source_id);


insert into top_cluster 
	select distinct * from full_cluster 
	where (smlty_name_cluster>.55 and smlty_name_cluster<1) or (smlty_addr_cluster>.55 and smlty_addr_cluster<1)
	order by mtph_name_cluster asc;

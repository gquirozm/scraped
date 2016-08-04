--dropea tablas de trabajo usadas, en caso que se requieran re-generar
--DROP TABLE despegar_delta;
--DROP TABLE booking_delta;
--DROP TABLE price_delta;
--DROP TABLE despegar_mapeo;
--DROP TABLE price_mapeo;
--DROP TABLE booking_mapeo;
--DROP TABLE full_cluster_tmp01;
--DROP TABLE full_cluster;
--DROP TABLE full_cluster_delta;
--DROP TABLE top_cluster;

--dropea funciones de trabajo usadas, en caso que se requieran re-generar 
--DROP FUNCTION create_table_delta()
--DROP FUNCTION create_table_mapeos()
--DROP FUNCTION create_table_cluster()
--DROP FUNCTION lkp_link_fetch(clav_hotel text);
--DROP FUNCTION LkpExists(clav_hotelportal text, link text); 
--DROP FUNCTION expReplace(value text); 
--DROP FUNCTION expHash(value text); 



CREATE OR REPLACE FUNCTION expHash(value text) RETURNS text AS
$$
    import hashlib
    m = hashlib.md5()
    m.update(value)
    return m.hexdigest()
$$
LANGUAGE 'plpythonu' VOLATILE;

CREATE OR REPLACE FUNCTION expReplace(line_in text, busca text, cadena text) RETURNS text AS
$$
    import string

    return string.replace(line_in, busca, cadena)
$$
LANGUAGE 'plpythonu' VOLATILE;


CREATE OR REPLACE FUNCTION LkpExists(clav_hotelportal text, link text) RETURNS boolean AS
$$
    import re
    clav_hotelportal_tmp = clav_hotelportal.lower()
    regex = re.compile(r'\W'+ clav_hotelportal_tmp +'\W')
    match = regex.search(link)
    if match:
	flag = 1
    else:
	flag = 0
    return flag
$$
LANGUAGE 'plpythonu' VOLATILE;

CREATE OR REPLACE FUNCTION lkp_link_fetch(clav_hotelportal text) RETURNS text AS 
$$
    import re
    rows = plpy.execute("select link from despegar")
    regex = re.compile(r'\W'+ clav_hotelportal +'\W')
    for i in range(len(rows)):
	 searchObj = regex.search(link)
	 if searchObj:
	  odd = rows[i]['link']
	 else:
	  odd = 'NULL'

    return odd
$$ 
LANGUAGE plpythonu;


CREATE OR REPLACE FUNCTION create_table_mapeo() RETURNS text AS 
$func$
BEGIN
EXECUTE format('CREATE TABLE IF NOT EXISTS %I (
		id serial 
		,city text
		,name text
		,address text
		,link text
		,latitud text
		,longitud text
		,source text
		,mapeo text
		,clav_hotel text
		,nombre_hotel text
		,clav_hotelportal text
		,date date)', 'despegar_mapeo'); 


EXECUTE format('CREATE TABLE IF NOT EXISTS %I (
		id serial 
		,city text
		,name text
		,address text
		,link text
		,latitud text
		,longitud text
		,source text
		,mapeo text
		,clav_hotel text
		,nombre_hotel text
		,clav_hotelportal text
		,date date)', 'price_mapeo'); 

EXECUTE format('CREATE TABLE IF NOT EXISTS %I (
		id serial 
		,city text
		,name text
		,address text
		,link text
		,latitud text
		,longitud text
		,source text
		,mapeo text
		,clav_hotel text
		,nombre_hotel text
		,clav_hotelportal text
		,date date)', 'booking_mapeo'); 
RETURN 'TABLAS MAPEOS GENERADAS';
END
$func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_table_delta() RETURNS text AS 
$func$
BEGIN
EXECUTE format('CREATE TABLE IF NOT EXISTS %I ( 
		 id serial
		,city text
		,name text
		,address text
		,link text
		,latitud text
		,longitud text
		,source text
		,mapeo text
		,hash text			
		,clav_hotel text
		,nombre_hotel text
		,clav_hotelportal text
		,date date)', 'booking_delta');
EXECUTE format('CREATE TABLE IF NOT EXISTS %I ( 
		 id serial
		,city text
		,name text
		,address text
		,link text
		,latitud text
		,longitud text
		,source text
		,mapeo text
		,hash text			
		,clav_hotel text
		,nombre_hotel text
		,clav_hotelportal text
		,date date)', 'despegar_delta');


EXECUTE format('CREATE TABLE IF NOT EXISTS %I ( 
		 id serial
		,city text
		,name text
		,address text
		,link text
		,latitud text
		,longitud text
		,source text
		,mapeo text
		,hash text			
		,clav_hotel text
		,nombre_hotel text
		,clav_hotelportal text
		,date date)', 'price_delta');
RETURN 'TABLAS DELTAS GENERADAS';
END
$func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION create_table_cluster() RETURNS text AS 
$func$
BEGIN
EXECUTE format('CREATE TABLE IF NOT EXISTS %I (
			m_id serial, 
			m_city text,
			m_name text, 
			m_address text,
			m_zipcode text,
			d_city text, 
			d_name text, 
			d_address text, 
			mtph_name_cluster text, 
			mtph_addr_cluster text, 
			smlty_name_cluster real, 
			smlty_addr_cluster real,
			hash text, 
			source_id text,
			date date)', 'full_cluster_tmp01');

EXECUTE format('CREATE TABLE IF NOT EXISTS %I (
		id serial, 
		city text,
		name text,
		address text,
		mapeo text,
		hash text,
		mtph_name_cluster text,
		mtph_addr_cluster text,
		d_city text, 
		d_name text, 
		d_address text, 
		smlty_name_cluster real, 
		smlty_addr_cluster real,
		source_id text,
		date date)', 'full_cluster');


EXECUTE format('CREATE TABLE IF NOT EXISTS %I (
		id serial,
		cluster_id text, 
		city text,
		name text,
		address text,
		link text,
		latitud text,
		longitud text,
		mapeo text,
		hash text,
		mtph_name_cluster text,
		mtph_addr_cluster text,
		d_city text, 
		d_name text, 
		d_address text, 
		smlty_name_cluster real, 
		smlty_addr_cluster real,
		source_id text,
		date date)', 'full_cluster_delta');

	
EXECUTE format('CREATE TABLE IF NOT EXISTS %I (
		id serial, 
		city text,
		name text,
		address text,
		mapeo text,
		hash text,
		mtph_name_cluster text,
		mtph_addr_cluster text,
		d_city text, 
		d_name text, 
		d_address text, 
		smlty_name_cluster real, 
		smlty_addr_cluster real,
		source_id text,
		date date)', 'top_cluster');
RETURN 'TABLAS CLUSTER GENERADAS';
END
$func$ LANGUAGE plpgsql;

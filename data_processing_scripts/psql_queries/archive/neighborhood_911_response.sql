drop table if exists neighborhood_911_response;

create table neighborhood_911_response as (
	select
		rsp.*
		, geo.ogc_fid
		, geo.objectid
		, geo.hoods_
		, geo.hoods_id
		, geo.s_hood
		, geo.l_hood
		, geo.l_hoodid
	from
		new_911_response as rsp
		left outer join
		neighborhoods as geo
			on ST_Contains(ST_Envelope(geo.wkb_geometry), rsp.geog::geometry)
)
;
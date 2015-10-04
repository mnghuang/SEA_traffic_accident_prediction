--create extension postgis;
drop table if exists new_911_response;

create table new_911_response as (
	with data as (
		select
			cad_cdw_id
			, at_scene_time
			, cad_event_number
			, census_tract
			, district_sector
			, event_clearance_code
			, event_clearance_date
			, event_clearance_description
			, event_clearance_group
			, event_clearance_subgroup
			, general_offense_number
			, hundred_block_location
			, incident_location
			, initial_type_description
			, initial_type_group
			, initial_type_subgroup
			, zone_beat
			, round(longitude::numeric, 2) as longitude
			, round(latitude::numeric, 2) as latitude
		from
			raw_911_response
		where
			longitude != ' '
			and latitude != ' '
	)
	select 
		*
		, ST_GeogFromText('SRID=4326;POINT(' || longitude || ' ' || latitude || ')') as geog
	from 
		data
)
;

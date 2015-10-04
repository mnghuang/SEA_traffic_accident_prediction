drop view if exists zone_latlong;

create view zone_latlong as (
	with zone_coords as (
		select distinct 
			trim(both ' ' from zone_beat) as zone_beat
			, latitude::float as latitude
			, longitude::float as longitude
			, row_number() over (partition by zone_beat
								 order by latitude) as lat_id
			, row_number() over (partition by zone_beat
								 order by longitude) as lon_id
		from 
			raw_911_response 
		where
			event_clearance_description = 'MOTOR VEHICLE COLLISION'
	)
	, max_latlon_id as (
		select
			zone_beat
			, max(lat_id) as max_lat_id
			, max(lon_id) as max_lon_id
		from
			zone_coords
		group by
			zone_beat
	)
	, mean_zone_coords as (
		select
			zc.zone_beat
			, sum(case when zc.lat_id = mli.max_lat_id / 2 then zc.latitude
				else 0 end) as med_lat
			, sum(case when zc.lon_id = mli.max_lon_id / 2 then zc.longitude
				else 0 end) as med_lon
		from
			zone_coords as zc
			join
			max_latlon_id as mli
				on zc.zone_beat = mli.zone_beat
		group by
			zc.zone_beat
	)
	, zone_coords_ranked as (
		select
			zc.zone_beat
			, zc.latitude
			, zc.longitude
			, row_number() over (partition by zc.zone_beat
								 order by sqrt((zc.latitude - mzc.med_lat)^2 +
								 		  (zc.longitude - mzc.med_lon)^2)) as row_id
		from
			zone_coords as zc
			join
			mean_zone_coords as mzc
				on zc.zone_beat = mzc.zone_beat
	)
	select
		zone_beat
		, latitude
		, longitude
	from
		zone_coords_ranked
	where
		row_id	= 1
)
;
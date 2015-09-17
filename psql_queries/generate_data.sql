with road_weather_json as (
	select
		datetime::date as event_date
		, airtemperature
		, roadsurfacetemperature
		, replace(
			replace(
				replace(
					replace(stationlocation, 'u''', '''')
					, '''', '"')
				, 'False' , '"False"')
			, 'True', '"True"')::json as json_data
	from 
		raw_road_weather 
)
, road_weather as (
	select
		event_date
		, replace(json_data ->> 'latitude', '"', '')::float as latitude
		, replace(json_data ->> 'longitude', '"', '')::float as longitude
		, min(airtemperature) as min_air_temp
		, max(airtemperature) as max_air_temp
		, min(roadsurfacetemperature) as min_road_temp
		, max(roadsurfacetemperature) as max_road_temp
	from
		road_weather_json
	group by
		1, 2, 3
)
select
	event_clearance_date::date as event_date
	, event_clearance_description as event_type
	, latitude
	, longitude
from
	raw_911_response as r9r
	join
	road_weather as rw
		on r9r.event_date = rw.event_date

;


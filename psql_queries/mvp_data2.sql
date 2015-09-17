with weather_transform as (
	select
		date::date as event_date
		, date_part('hour', (date || ' ' ||time)::timestamp) as hour
		, conditions
		, (regexp_matches(dewpoint, '\d+\.\d+'))[1]::float as dewpoint
		, case when events like '%Fog%' then 1 else 0 end as fog
		, case when events like '%Rain%' then 1 else 0 end as rain
		, case when events like '%Thunderstorm%' then 1 else 0 end as thunderstorm
		, case when events like '%Snow%' then 1 else 0 end as snow
		, case
			when gustspeed = '-' then 0
			else (regexp_matches(gustspeed, '\d+\.\d+'))[1]::float
		end as gustspeed
		, case
			when heatindex = '-' or heatindex is null then 0 
			else (regexp_matches(heatindex, '\d+\.\d+'))[1]::float
		end as heatindex
		, (regexp_matches(humidity, '\d+'))[1]::float as humidity
		, case
			when precip = 'N/A' then 0
			else (regexp_matches(precip, '\d+\.\d+'))[1]::float
		end as precip
		, (regexp_matches(pressure, '\d+\.\d+'))[1]::float as pressure
		, (regexp_matches(temp, '\d+\.\d+'))[1]::float as temp
		, (regexp_matches(visibility, '\d+\.\d+'))[1]::float as visibility
		, winddir
		, case
			when windspeed = 'Calm' then 0 
			else (regexp_matches(windspeed, '\d+\.\d+'))[1]::float
		end as windspeed
		, row_number() over (partition by date::date
							 , date_part('hour', (date || ' ' ||time)::timestamp)
							 order by date_part('minute', (date || ' ' ||time)::timestamp)
							) as row_id
	from
		weather
)
, weather_data as (
	select 
		event_date
		, hour
		, conditions
		, dewpoint
		, fog
		, rain
		, thunderstorm
		, snow
		, gustspeed
		, heatindex
		, humidity
		, precip
		, pressure
		, temp
		, visibility
		, winddir
		, windspeed
	from
		weather_transform
	where
		row_id = 1
)
, response_911 as (
	select distinct
		event_clearance_date::date as event_date
		, date_part('hour', event_clearance_date::timestamp) as hour
		, zone_beat
	from
		raw_911_response
	where
		event_clearance_description = 'MOTOR VEHICLE COLLISION'
)
, mariner_games as (
	select
		date_box_score::date as event_date
	from
		mariner_schedule
	where
		opponent like 'vs %'
)
, seahawk_games as (
	select
		date::date as event_date
	from
		seahawk_schedule
	where
		location like '%Seattle, WA%'
)
, date_hour as (
	select distinct
		event_date
		, hour
	from
		weather_data
)
, beats as (
	select distinct
		zone_beat
	from
		response_911
)
, date_beats as (
	select
		dh.event_date
		, dh.hour
		, b.zone_beat
	from
		date_hour as dh
		join
		beats as b
			on 1 = 1
)
select
	db.event_date
	, db.hour
	, db.zone_beat
	, case when mg.event_date is null then 0 else 1 end as mariner_plays
	, case when sg.event_date is null then 0 else 1 end as seahawk_plays
	, wd.conditions
	, wd.dewpoint
	, wd.fog
	, wd.rain
	, wd.thunderstorm
	, wd.snow
	, wd.gustspeed
	, wd.heatindex
	, wd.humidity
	, wd.precip
	, wd.pressure
	, wd.temp
	, wd.visibility
	, wd.winddir
	, wd.windspeed
	, case when rsp.event_date is null then 0 else 1 end as label
from
	date_beats as db
	left outer join
	mariner_games as mg
		on db.event_date = mg.event_date
	left outer join
	seahawk_games as sg
		on db.event_date = sg.event_date
	left outer join
	weather_data as wd
		on db.event_date = wd.event_date
		and db.hour = wd.hour
	left outer join
	response_911 as rsp
		on db.event_date = rsp.event_date
		and db.hour = rsp.hour
		and db.zone_beat = rsp.zone_beat
;
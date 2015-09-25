drop view if exists compiled_data;

create view compiled_data as (
	with weather_transform as (
		select
			date::date as event_date
			, date_part('hour', (date || ' ' ||time)::timestamp) as hour
			, date_part('dow', date::timestamp) as dow
			, date_part('month', date::timestamp) as month
			, conditions
			, (regexp_matches(dewpoint, '\d+\.\d+'))[1]::float as dewpoint
			--, case when events like '%Fog%' then 1 else 0 end as fog
			--, case when events like '%Rain%' then 1 else 0 end as rain
			--, case when events like '%Thunderstorm%' then 1 else 0 end as thunderstorm
			--, case when events like '%Snow%' then 1 else 0 end as snow
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
			, dow
			, month
			, conditions
			, dewpoint
			, case 
				when lag(dewpoint, 12) over (order by event_date, hour) is null
				then 0
				else dewpoint - lag(dewpoint, 12) over (order by event_date, hour)
			end dewpoint_change
			, gustspeed
			, case 
				when lag(gustspeed, 12) over (order by event_date, hour) is null
				then 0
				else gustspeed - lag(gustspeed, 12) over (order by event_date, hour)
			end gustspeed_change
			, heatindex
			, case 
				when lag(heatindex, 12) over (order by event_date, hour) is null
				then 0
				else heatindex - lag(heatindex, 12) over (order by event_date, hour)
			end heatindex_change
			, humidity
			, case 
				when lag(humidity, 12) over (order by event_date, hour) is null
				then 0
				else humidity - lag(humidity, 12) over (order by event_date, hour)
			end humidity_change
			, precip
			, case 
				when lag(precip, 12) over (order by event_date, hour) is null
				then 0
				else precip - lag(precip, 12) over (order by event_date, hour)
			end precip_change
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
			, trim(both ' ' from zone_beat) as zone_beat
		from
			raw_911_response
		where
			event_clearance_date != ' '
			and event_clearance_code in ('430', '460')--'450', '460', '482')
			and trim(both ' ' from zone_beat) in (select category from zone_beat_id)
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
	 	zbm.id as zone_beat_id
	 	, cm.id as condition_id
	 	, wm.id as winddir_id
		, db.hour
		, wd.dow
		, wd.month
		, case when mg.event_date is null then 0 else 1 end as mariner_plays
		, case when sg.event_date is null then 0 else 1 end as seahawk_plays
		, wd.dewpoint
		, wd.gustspeed
		, wd.heatindex
		, wd.humidity
		, wd.precip
		, wd.pressure
		, wd.temp
		, wd.visibility
		, wd.windspeed
		, case when rsp.event_date is null then 0 else 1 end as label
	from
		date_beats as db
		left outer join
		mariner_plays as mg
			on db.event_date = mg.event_date
		left outer join
		seahawk_plays as sg
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
		left outer join
		zone_beat_id as zbm
			on db.zone_beat = zbm.category
		left outer join
		condition_id as cm
			on wd.conditions = cm.category
		left outer join
		winddir_id as wm
			on wd.winddir = wm.category
)
;
drop view if exists train_data;

create view train_data as (
	with weather_transform as (
		select
			date::date as event_date
			, date_part('hour', (date || ' ' ||time)::timestamp) as hour
			, date_part('dow', date::timestamp) as dow
			, date_part('month', date::timestamp) as month
			, conditions
			, (regexp_matches(dewpoint, '\d+\.\d+'))[1]::float as dewpoint
			, case when gustspeed = '-' then 0 else 1 end as have_gustspeed
			, case when heatindex = '-' or heatindex is null then 0 else 1 end as have_heatindex
			, (regexp_matches(humidity, '\d+'))[1]::float as humidity
			, case when precip = 'N/A' then 0 else 1 end as have_precip
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
			, have_gustspeed
			, have_heatindex
			, humidity
			, have_precip
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
		select
			event_clearance_date::date as event_date
			, date_part('hour', event_clearance_date::timestamp) as hour
			, trim(both ' ' from zone_beat) as zone_beat
			, count(*) as number_accidents
		from
			raw_911_response
		where
			event_clearance_date != ' '
			and event_clearance_code in ('430', '460')--'450', '460', '482')
			and trim(both ' ' from zone_beat) in (select category from zone_beat_id)
		group by
			event_clearance_date::date
			, date_part('hour', event_clearance_date::timestamp)
			, trim(both ' ' from zone_beat)
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
			trim(both ' ' from zone_beat) as zone_beat
			, hundred_block_location
		from
			raw_911_response
		where
			event_clearance_date != ' '
			and event_clearance_code in ('430', '460')--'450', '460', '482')
			and trim(both ' ' from zone_beat) in (select category from zone_beat_id)
	)
	, new_beats as (
		select
			zone_beat
			, sum(case when hundred_block_location like '%/%' then 1 else 0 end) as intersections
		from
			beats
		group by 
			zone_beat
	)
	, date_beats as (
		select
			dh.event_date
			, dh.hour
			, b.zone_beat
			, b.intersections
		from
			date_hour as dh
			join
			new_beats as b
				on 1 = 1
	)
	select
	 	zbm.id as zone_beat_id
	 	, cm.id as condition_id
	 	, wm.id as winddir_id
	 	, db.intersections
		, db.hour
		, wd.dow
		, wd.month
		, case when mg.event_date is null then 0 else 1 end as mariner_plays
		, case when sg.event_date is null then 0 else 1 end as seahawk_plays
		, wd.dewpoint
		, wd.have_gustspeed
		, wd.have_heatindex
		, wd.humidity
		, wd.have_precip
		, wd.pressure
		, wd.temp
		, wd.visibility
		, wd.windspeed
		, case when rsp.event_date is not null then 1 else 0 end as label
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
	where
		db.event_date between '2013-01-01' and '2014-12-31'
)
;
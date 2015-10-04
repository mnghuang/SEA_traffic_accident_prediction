drop view if exists glm_data;

create view glm_data as (
	with response_911 as (
		select
			s_hood
			, event_clearance_date::date as event_date
			, date_part('hour', event_clearance_date::timestamp) + 1 as hour
			, case when date_part('dow', event_clearance_date::timestamp) = 0 then 1
				else date_part('dow', event_clearance_date::timestamp) + 1 end as dow
			, date_part('week', event_clearance_date::timestamp) as week
			, case when hundred_block_location like '%/%' then 1 else 0 end as is_intersection
			, count(*) as number_accidents
		from
			neighborhood_911_response
		where
			event_clearance_date != ' '
			and event_clearance_code in ('430', '460')
			and s_hood is not null
		group by
			s_hood
			, event_clearance_date::date
			, date_part('hour', event_clearance_date::timestamp) + 1
			, date_part('dow', event_clearance_date::timestamp) 
			, date_part('week', event_clearance_date::timestamp) 
			, case when hundred_block_location like '%/%' then 1 else 0 end
	)
	, weather as (
		select
			date::date as event_date
			, date_part('hour', (date || ' ' ||time)::timestamp) + 1 as hour
			, (regexp_matches(dewpoint, '\d+\.\d+'))[1]::float as dewpoint
			, case when gustspeed = '-' then 0 else 1 end as have_gustspeed
			, case when heatindex = '-' or heatindex is null then 0 else 1 end as have_heatindex
			, (regexp_matches(humidity, '\d+'))[1]::float as humidity
			, case when precip = 'N/A' then 0 else 1 end as have_precip
			, (regexp_matches(pressure, '\d+\.\d+'))[1]::float as pressure
			, (regexp_matches(temp, '\d+\.\d+'))[1]::float as temp
			, (regexp_matches(visibility, '\d+\.\d+'))[1]::float as visibility
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
	, weather_dedup as (
		select 
			event_date
			, hour
			, dewpoint
			, have_gustspeed
			, have_heatindex
			, humidity
			, have_precip
			, pressure
			, temp
			, visibility
			, windspeed
		from
			weather
		where
			row_id = 1
	)
	select
		rsp.s_hood
		, rsp.event_date
		, rsp.hour
		, rsp.dow
		, rsp.week
		, rsp.is_intersection
		, wth.dewpoint
		, wth.have_gustspeed
		, wth.have_heatindex
		, wth.humidity
		, wth.have_precip
		, wth.pressure
		, wth.temp
		, wth.visibility
		, wth.windspeed
		, rsp.number_accidents
	from
		response_911 as rsp
		join
		weather as wth
			on rsp.event_date = wth.event_date
			and rsp.hour = wth.hour
);

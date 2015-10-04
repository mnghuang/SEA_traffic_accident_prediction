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
select
	rsp.event_clearance_date::date as event_date
	, date_part('hour', rsp.event_clearance_date::timestamp) as hour
	, wd.pressure
	, wd.conditions
	, wd.windspeed
from
	raw_911_response as rsp
	right outer join
	weather_data as wd
		on rsp.event_clearance_date::date = wd.event_date
		and date_part('hour', rsp.event_clearance_date::timestamp) = wd.hour
where
	rsp.event_clearance_date != ' '
	and rsp.event_clearance_code in ('430', '460')
	and rsp.event_clearance_date::date > '2014-12-31'
;

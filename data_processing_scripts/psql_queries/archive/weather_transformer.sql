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
			when gustspeed = '-' then null 
			else (regexp_matches(gustspeed, '\d+\.\d+'))[1]::float
		end as gustspeed
		, case
			when heatindex = '-' or heatindex is null then null 
			else (regexp_matches(heatindex, '\d+\.\d+'))[1]::float
		end as heatindex
		, (regexp_matches(humidity, '\d+'))[1]::float as humidity
		, case
			when precip = 'N/A' then null 
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
select
	count(distinct hour)
from
	weather_transform
where
	row_id = 1
;

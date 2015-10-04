drop table if exists zone_beat_id cascade;

create table zone_beat_id as (
	with zone_beats as (
		select distinct
			trim(both ' ' from zone_beat) as zone_beat
		from 
			raw_911_response
		where
			event_clearance_description = 'MOTOR VEHICLE COLLISION'
	)
	select
		zone_beat as category
		, row_number() over (order by zone_beat) as id
	from
		zone_beats
)
;

drop table if exists condition_id cascade;

create table condition_id as (
	with condition_list as (
		select distinct
			conditions
			, case
				when conditions like '%Snow%' then 'Snow'
				when conditions like '%Rain%' or conditions like '%Drizzle%' then 'Rain'
				when conditions like '%Fog%' 
					or conditions like '%Smoke%'
					or conditions like '%Haze%' then 'Fog'
				when conditions like '%Thunderstorm%' then 'Thunderstorm'
				when conditions like '%Cloud%'
					or conditions like '%Overcast%' then 'Cloudy'
				else 'Others'
			end as category
		from
			weather
	)
	, category_list as (
		select distinct
			case
				when conditions like '%Snow%' then 'Snow'
				when conditions like '%Rain%' or conditions like '%Drizzle%' then 'Rain'
				when conditions like '%Fog%' 
					or conditions like '%Smoke%'
					or conditions like '%Haze%' then 'Fog'
				when conditions like '%Thunderstorm%' then 'Thunderstorm'
				when conditions like '%Cloud%'
					or conditions like '%Overcast%' then 'Cloudy'
				else 'Others'
			end as category
		from
			weather
	)
	, category_list2 as (
		select 
			category
			, row_number() over (order by category) as id
		from
			category_list
	)
	select 
		con.conditions as category
		, cat.id
	from
		condition_list as con
		join
		category_list2 as cat
			on con.category = cat.category
)
;

drop table if exists winddir_id cascade;

create table winddir_id as (
	with winddirs as (
		select distinct
			winddir
		from
			weather
	)
	select
		winddir as category
		, row_number() over (order by winddir) as id
	from
		winddirs
)
;
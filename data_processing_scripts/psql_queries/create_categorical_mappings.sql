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
		from
			weather
	)
	select
		conditions as category
		, row_number() over (order by conditions) as id
	from
		condition_list
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
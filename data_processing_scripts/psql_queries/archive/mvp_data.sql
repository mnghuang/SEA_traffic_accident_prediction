with response_911 as (
	select distinct
		event_clearance_date::date as event_date
		, zone_beat
	from
		raw_911_response
	where
		event_clearance_description = 'MOTOR VEHICLE COLLISION'
)
, dates as (
	select distinct
		datetime::date as event_date
	from
		raw_road_weather 
)
, beats as (
	select distinct
		zone_beat
	from
		response_911
)
, date_beats as (
	select
		d.event_date
		, b.zone_beat
	from
		dates as d
		join
		beats as b
			on 1 = 1
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
select
	db.event_date
	, db.zone_beat
	, case when mg.event_date is null then 0 else 1 end as mariner_plays
	, case when sg.event_date is null then 0 else 1 end as seahawk_plays
	, case when rsp.event_date is null then 0 else 1 end as label
from
	date_beats as db
	left outer join
	response_911 as rsp
		on db.event_date = rsp.event_date
		and db.zone_beat = rsp.zone_beat
	left outer join
	mariner_games as mg
		on db.event_date = mg.event_date
	left outer join
	seahawk_games as sg
		on db.event_date = sg.event_date
;
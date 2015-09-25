-- Creating consolidated views from web scrapes

drop view if exists mariner_schedule cascade;

create view mariner_schedule as (
	select 
		game, date_box_score, opponent, score, decision, record
	from 
		sea_2009

	union all
	select
		game, date_box_score, opponent, score, decision, record
	from
		sea_2010

	union all
	select
		game, date_box_score, opponent, score, decision, record
	from 
		sea_2011

	union all
	select
		game, date_box_score, opponent, score, decision, record
	from
		sea_2012

	union all
	select
		game, date_box_score, opponent, score, decision, record
	from 
		sea_2013

	union all
	select
		game, date_box_score, opponent, score, decision, record
	from 
		sea_2014
	union all
	select 
		game, date as date_box_score, opponent, score, decision, record
	from
		sea_2015
)
;

drop view if exists seahawk_schedule cascade;

create view seahawk_schedule as (
	select * from seattle_seahawks_2009

	union all
	select * from seattle_seahawks_2010

	union all
	select * from seattle_seahawks_2011

	union all
	select * from seattle_seahawks_2012

	union all
	select * from seattle_seahawks_2013

	union all
	select * from seattle_seahawks_2014

	union all
	select * from seattle_seahawks_2015
)
;

drop view if exists mariner_plays cascade;

create view mariner_plays as (
	select
		date_box_score::date as event_date
	from
		mariner_schedule
	where
		opponent like 'vs %'
)
;

drop view if exists seahawk_plays cascade;

create view seahawk_plays as (
	select
		date::date as event_date
	from
		seahawk_schedule
	where
		location like '%Seattle, WA%'
)
;
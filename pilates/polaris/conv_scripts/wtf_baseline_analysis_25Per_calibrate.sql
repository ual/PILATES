ATTACH DATABASE "chicago2018-Supply.sqlite" as a;

DROP TABLE IF EXISTS Activity_Start_Distribution;
CREATE TABLE IF NOT EXISTS activity_Start_Distribution As
SELECT 
	cast(start_time/3600 as int),
	sum(CASE WHEN type= 'EAT OUT' THEN 1 END) as EAT_OUT,
	sum(CASE WHEN type= 'ERRANDS' THEN 1 END) as ERRANDS,
	sum(CASE WHEN type= 'HEALTHCARE' THEN 1 END) as HEALTHCARE,
	sum(CASE WHEN type= 'LEISURE' THEN 1 END) as LEISURE,
	sum(CASE WHEN type= 'PERSONAL' THEN 1 END) as PERSONAL,
	sum(CASE WHEN type= 'RELIGIOUS-CIVIC' THEN 1 END) as RELIGIOUS,
	sum(CASE WHEN type= 'SERVICE' THEN 1 END) as SERVICE,
	sum(CASE WHEN type= 'SHOP-MAJOR' THEN 1 END) as SHOP_MAJOR,
	sum(CASE WHEN type= 'SHOP-OTHER' THEN 1 END) as SHOP_OTHER,
	sum(CASE WHEN type= 'SOCIAL' THEN 1 END) as SOCIAL,
	sum(CASE WHEN type= 'WORK' THEN 1 END) as WORK,
	sum(CASE WHEN type= 'PART_WORK' THEN 1 END) as WORK_PART,
	sum(CASE WHEN type= 'WORK AT HOME' THEN 1 END) as WORK_HOME,
	sum(CASE WHEN type= 'SCHOOL' THEN 1 END) as SCHOOL,
	sum(CASE WHEN type= 'PICKUP-DROPOFF' THEN 1 END) as PICKUP,
	sum(CASE WHEN type= 'HOME' THEN 1 END) as HOME,
	sum(1) AS total
FROM 
	Activity
WHERE
	start_time > 122 and trip <> 0
GROUP BY 
	cast (start_time/3600 as int);


DROP TABLE IF EXISTS TTime_By_ACT_Average;
CREATE TABLE IF NOT EXISTS ttime_By_ACT_Average As
select activity.type as acttype, avg(trip.end - trip.start)/60 as ttime_avg, avg(trip.travel_distance)/1609.3 as dist_avg
from trip, person, household, activity
where trip.person = person.person and person.household = household.household and person.age > 16 and activity.trip = trip.trip_id and
trip.end - trip.start > 0 and trip.end - trip.start < 10800
group by ACTTYPE;


DROP TABLE IF EXISTS Mode_Distribution_ADULT;
CREATE TABLE IF NOT EXISTS mode_Distribution_ADULT As
select mode, 
(sum(case when trip.destination = person.work_location_id then 1 else 0 end))*4 as 'HBW',
(sum(case when trip.origin = household.location and trip.destination <> person.work_location_id  then 1 else 0 end))*4 as 'HBO',
(sum(case when trip.origin <> household.location and trip.destination <> household.location and trip.destination <> person.work_location_id then 1 else 0 end))*4 as 'NHB',
(count(*))*4 as total
from trip, person, household
where trip.person = person.person and person.household = household.household and person.age > 16
group by mode;

drop table if exists boardings_by_agency_mode;
create table boardings_by_agency_mode as
SELECT 
	tr.agency as agency, 
	tv.mode as mode, 
	4*sum(tvl.value_boardings) as boardings, 
	4*sum(tvl.value_alightings) as alightings
FROM 
	"Transit_Vehicle_links" tvl, 
	transit_vehicle tv, 
	a.transit_trips tt, 
	a.transit_patterns tp, 
	a.transit_routes tr
where 
	tvl.value_transit_vehicle_trip_id = tv.transit_vehicle_trip_id and 
	tvl.value_transit_vehicle_trip = tt.trip and 
	tp.pattern = tt.pattern and 
	tr.route = tp.route
group by 
	agency,
	mode
order by
	agency,
	mode desc 
;


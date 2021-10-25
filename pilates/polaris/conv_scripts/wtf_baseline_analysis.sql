DROP TABLE IF EXISTS person_distribution;
CREATE TABLE IF NOT EXISTS person_distribution As
SELECT
case when (employment = 4 or employment = 1) and work_hours >= 30 then 'FULLTIME_WORKER'
    when (employment = 4 or employment = 1) then 'PARTTIME_WORKER'
    when (school_enrollment = 3 or school_enrollment = 2) and age > 18 then 'ADULT_STUDENT'
    when age >= 65 then 'SENIOR'
    when age < 65 and age > 18 then 'NONWORKER'
    when age >= 16 and age <= 18 then 'STUDENT_DRIVER'
    when age < 16 and age >= 5 then 'SCHOOL_CHILD'
    when age < 5 then 'PRESCHOOL'
    else 'NONWORKER'
end as pertype,
cast(count(*) as real) AS total
from person
GROUP BY
    pertype;
	
DROP TABLE IF EXISTS activity_distribution;
CREATE TABLE IF NOT EXISTS activity_distribution As
SELECT
case when (employment = 4 or employment = 1) and work_hours >= 30 then 'FULLTIME_WORKER'
    when (employment = 4 or employment = 1) then 'PARTTIME_WORKER'
    when (school_enrollment = 3 or school_enrollment = 2) and age > 18 then 'ADULT_STUDENT'
    when age >= 65 then 'SENIOR'
    when age < 65 and age > 18 then 'NONWORKER'
    when age >= 16 and age <= 18 then 'STUDENT_DRIVER'
    when age < 16 and age >= 5 then 'SCHOOL_CHILD'
    when age < 5 then 'PRESCHOOL'
    else 'NONWORKER'
end as pertype,
CASE WHEN type= 'EAT OUT' THEN 'EAT_OUT'
 WHEN type= 'ERRANDS' THEN 'ERRANDS'
 WHEN type= 'HEALTHCARE' THEN 'HEALTHCARE'
 WHEN type= 'HOME' THEN 'HOME'
 WHEN type= 'LEISURE' THEN 'LEISURE'
 WHEN type= 'PERSONAL' THEN 'PERSONAL_BUSINESS'
 WHEN type= 'PICKUP-DROPOFF' THEN 'PICKUP_DROPOFF'
 WHEN type= 'RELIGIOUS-CIVIC' THEN 'RELIGIOUS_OR_CIVIC'
 WHEN type= 'SCHOOL' THEN 'SCHOOL'
 WHEN type= 'SERVICE' THEN 'SERVICE_VEHICLE'
 WHEN type= 'SHOP-MAJOR' THEN 'MAJOR_SHOPPING'
 WHEN type= 'SOCIAL' THEN 'SOCIAL'
 WHEN type= 'WORK' THEN 'WORK'
 WHEN type= 'WORK AT HOME' THEN 'WORK'
 WHEN type= 'PART_WORK' THEN 'PART_TIME_WORK'
 WHEN type= 'SHOP-OTHER' THEN 'OTHER_SHOPPING'
END AS acttype,
cast(count(*) as real) as count
from person p, activity a
where p.person = a.person and a.Start_Time > 122 and a.trip <> 0 and not (a.mode = 'NO_MOVE' and a.type in ('HOME','WORK AT HOME','PICKUP-DROPOFF'))
GROUP BY pertype, acttype;

drop table if exists activity_rate_distribution;
create table activity_rate_distribution as
select a.pertype as pertype, a.acttype as acttype, a.count/p.total as rate
from activity_distribution a, person_distribution p
where a.pertype = p.pertype
order by pertype, acttype;

DROP TABLE IF EXISTS Activity_Start_Distribution;
CREATE TABLE IF NOT EXISTS activity_Start_Distribution As
SELECT 
	cast(start_time/3600 as int),
	4*sum(CASE WHEN type= 'EAT OUT' THEN 1 END) as EAT_OUT,
	4*sum(CASE WHEN type= 'ERRANDS' THEN 1 END) as ERRANDS,
	4*sum(CASE WHEN type= 'HEALTHCARE' THEN 1 END) as HEALTHCARE,
	4*sum(CASE WHEN type= 'LEISURE' THEN 1 END) as LEISURE,
	4*sum(CASE WHEN type= 'PERSONAL' THEN 1 END) as PERSONAL,
	4*sum(CASE WHEN type= 'RELIGIOUS-CIVIC' THEN 1 END) as RELIGIOUS,
	4*sum(CASE WHEN type= 'SERVICE' THEN 1 END) as SERVICE,
	4*sum(CASE WHEN type= 'SHOP-MAJOR' THEN 1 END) as SHOP_MAJOR,
	4*sum(CASE WHEN type= 'SHOP-OTHER' THEN 1 END) as SHOP_OTHER,
	4*sum(CASE WHEN type= 'SOCIAL' THEN 1 END) as SOCIAL,
	4*sum(CASE WHEN type= 'WORK' THEN 1 END) as WORK,
	4*sum(CASE WHEN type= 'PART_WORK' THEN 1 END) as WORK_PART,
	4*sum(CASE WHEN type= 'WORK AT HOME' THEN 1 END) as WORK_HOME,
	4*sum(CASE WHEN type= 'SCHOOL' THEN 1 END) as SCHOOL,
	4*sum(CASE WHEN type= 'PICKUP-DROPOFF' THEN 1 END) as PICKUP,
	4*sum(CASE WHEN type= 'HOME' THEN 1 END) as HOME,
	4*sum(1) AS total
FROM 
	Activity
WHERE
	start_time > 122 and trip <> 0 and not (mode = 'NO_MOVE' and type in ('HOME','WORK AT HOME','PICKUP-DROPOFF'))
GROUP BY 
	cast (start_time/3600 as int);
	
drop table if exists vmt_vht_by_mode;
create table vmt_vht_by_mode as
SELECT mode, 4*sum(travel_distance)/1609.3/1000000.0 as million_VMT, 4*sum(end-start)/3600.0/1000000.0 as million_VHT, 4*sum(travel_distance)/1609.3/(4*sum(end-start)/3600.0) as speed_mph, 4*count(*) as count
FROM trip
where has_artificial_trip <> 1
group BY mode;

drop table if exists vmt_vht_by_mode_city;
create table vmt_vht_by_mode_city as
SELECT mode, 4*sum(travel_distance)/1609.3/1000000.0 as million_VMT, 4*sum(end-start)/3600.0/1000000.0 as million_VHT, 4*sum(travel_distance)/1609.3/(4*sum(end-start)/3600.0) as speed_mph, 4*count(*) as count
FROM trip t, a.location as al, a.zone as az
where t."end" > t.start and t.origin = al.location and al.zone = az.zone and az.area_type <= 3 and has_artificial_trip <> 1
group BY mode;

drop table if exists trips_in_network_city;
create table trips_in_network_city as
select time, cum_depart, cum_arrive, cum_depart-cum_arrive as in_network from
(
select time, 4*sum(departures) OVER (ROWS UNBOUNDED PRECEDING) as cum_depart, 4*sum(arrivals) OVER (ROWS UNBOUNDED PRECEDING) as cum_arrive from
(
select time, sum(departures) as departures, sum(arrivals) as arrivals from
(
select cast("start"/6 as int)*6 as time, count(*) as departures, 0 as arrivals
from trip t, a.location l1, a.location l2
where mode = 0 and person is not null and t.origin = l1.location and t.destination = l2.location and (l1.area_type <= 3 or l2.area_type <= 3)
group by time
union
select cast("end"/6 as int)*6 as time, 0 as departures, count(*) as arrivals
from trip t, a.location l1, a.location l2
where mode = 0 and person is not null and t.origin = l1.location and t.destination = l2.location and (l1.area_type <= 3 or l2.area_type <= 3)
group by time
union
select cast("start"/6 as int)*6 as time, count(*) as departures, 0 as arrivals
from tnc_trip t, a.location l1, a.location l2
where t.origin = l1.location and t.destination = l2.location and (l1.area_type <= 3 or l2.area_type <= 3)
group by time
union
select cast("end"/6 as int)*6 as time, 0 as departures, count(*) as arrivals
from tnc_trip t, a.location l1, a.location l2
where t.origin = l1.location and t.destination = l2.location and (l1.area_type <= 3 or l2.area_type <= 3)
group by time
)
group by time
)
)
;

drop table if exists planned_activity_mode_share;
create table planned_activity_mode_share as
Select
	activity.mode, 4*count(*) as mode_count
FROM
	activity, person
WHERE
	activity.start_time > 122 and 
	activity.trip = 0 and
	activity.person = person.person and
	person.age > 16
GROUP BY
	activity.mode;

drop table if exists executed_activity_mode_share;
create table executed_activity_mode_share as
Select
	activity.mode as mode, 4*count(*) as mode_count
FROM
	activity, person, trip
WHERE
	activity.start_time > 122 and 
	activity.trip = trip.trip_id and
	trip."end" - trip."start" > 2 and
	activity.person = person.person and
	person.age > 16 and
	activity.mode not like 'FAIL%'
GROUP BY
	activity.mode;

drop table if exists executed_activity_mode_share_fails;
create table executed_activity_mode_share_fails as
Select
	activity.mode as mode, 4*count(*) as mode_count
FROM
	activity, person
WHERE
	activity.start_time > 122 and 
	activity.trip <> 0 and
	activity.person = person.person and
	person.age > 16
GROUP BY
	activity.mode;

drop table if exists planned_activity_mode_share_by_area;
create table planned_activity_mode_share_by_area as
Select
    activity.type, a.zone.area_type, activity.mode, 4*count(*) as mode_count
FROM
    activity, person, a.location, a.zone
WHERE
    activity.start_time > 122 and
    activity.trip = 0 and
    activity.person = person.person and
    person.age > 16 and
	activity.location_id = a.location.location and a.location.zone = a.zone.zone
GROUP BY
    activity.type, a.zone.area_type, activity.mode;
	
drop table if exists executed_activity_mode_share_by_area;
create table executed_activity_mode_share_by_area as
Select
    activity.type, a.zone.area_type, activity.mode, 4*count(*) as mode_count
FROM
    activity, person, a.location, a.zone, trip 
WHERE
    activity.start_time > 122 and
	activity.trip = trip.trip_id and
    activity.person = person.person and
    person.age > 16 and
	activity.location_id = a.location.location and a.location.zone = a.zone.zone
GROUP BY
    activity.type, a.zone.area_type, activity.mode;
	
drop table if exists executed_activity_dist_by_area;
create table executed_activity_dist_by_area as
select type, sum(mode_count) as mode_count 
from "executed_activity_mode_share_by_area"
group by type;

drop table if exists executed_activity_dist_by_area_city;
create table executed_activity_dist_by_area_city as
select type, sum(mode_count) as mode_count 
from "executed_activity_mode_share_by_area"
where area_type < 4
group by type;
	
DROP TABLE IF EXISTS ttime_By_ACT_Average;
CREATE TABLE IF NOT EXISTS ttime_By_ACT_Average As
select activity.type as acttype, avg(trip.skim_travel_time)/60 as ttime_avg_skim, avg(trip.routed_travel_time)/60 as ttime_avg_routed, avg(trip.end - trip.start)/60 as ttime_avg, avg(trip.travel_distance)/1000 as dist_avg, 4*count(*) as count
from trip, person, household, activity
where trip.person = person.person and person.household = household.household and person.age > 16 and activity.trip = trip.trip_id and travel_distance < 1000000 and
trip.end - trip.start > 2
group by ACTTYPE;

DROP TABLE IF EXISTS ttime_By_ACT_Average_w_skims;
CREATE TABLE IF NOT EXISTS ttime_By_ACT_Average_w_skims As
select activity.type as acttype, avg(trip.skim_travel_time)/60 as ttime_avg_skim, avg(trip.routed_travel_time)/60 as ttime_avg_routed, avg(trip.end - trip.start)/60 as ttime_avg, avg(trip.travel_distance)/1000 as dist_avg, 4*count(*) as count
from trip, person, household, activity
where trip.person = person.person 
and person.household = household.household 
and person.age > 16 
and activity.trip = trip.trip_id 
and travel_distance < 1000000 
and trip.end - trip.start >= 0
and trip.skim_travel_time >= 0
and trip.skim_travel_time < 86400
group by ACTTYPE;

DROP TABLE IF EXISTS ttime_By_ACT_Average_w_skims_hway;
CREATE TABLE IF NOT EXISTS ttime_By_ACT_Average_w_skims_hway As
select trip.mode, activity.type as acttype, avg(trip.skim_travel_time)/60 as ttime_avg_skim, avg(trip.routed_travel_time)/60 as ttime_avg_routed, avg(trip.end - trip.start)/60 as ttime_avg, avg(trip.travel_distance)/1000 as dist_avg, 4*count(*) as count
from trip, person, household, activity
where trip.person = person.person 
and person.household = household.household 
and person.age > 16 
and activity.trip = trip.trip_id 
and travel_distance < 1000000 
and trip.end - trip.start >= 0
and trip.skim_travel_time >= 0
and trip.skim_travel_time < 86400
and trip.mode in (0,9)
group by trip.mode, ACTTYPE;

DROP TABLE IF EXISTS Mode_Distribution_ADULT;
CREATE TABLE IF NOT EXISTS mode_Distribution_ADULT As
select mode, 
4*(sum(case when trip.destination = person.work_location_id then 1 else 0 end)) as 'HBW',
4*(sum(case when trip.origin = household.location and trip.destination <> person.work_location_id  then 1 else 0 end)) as 'HBO',
4*(sum(case when trip.origin <> household.location and trip.destination <> household.location and trip.destination <> person.work_location_id then 1 else 0 end)) as 'NHB',
4*(count(*)) as total
from trip, person, household
where trip.person = person.person and person.household = household.household and person.age > 16 and trip."end" - trip."start" > 2
group by mode;


drop table if exists boardings_by_agency_mode;
create table boardings_by_agency_mode as
SELECT 
	ta.agency as agency,
	tv.mode as mode, 
	4*sum(tvl.value_boardings) as boardings, 
	4*sum(tvl.value_alightings) as alightings
FROM 
	"Transit_Vehicle_links" tvl, 
	transit_vehicle tv, 
	a.transit_trips tt, 
	a.transit_patterns tp, 
	a.transit_routes tr,
	a.transit_agencies ta
where 
	tvl.value_transit_vehicle_trip = tv.transit_vehicle_trip and 
	tvl.value_transit_vehicle_trip = tt.trip_id and 
	tp.pattern_id = tt.pattern_id and 
	tr.route_id = tp.route_id AND
	tr.agency_id = ta.agency_id
group by 
	ta.agency,
	tv.mode
order by
	ta.agency,
	tv.mode desc 
;

drop table if exists boardings_by_agency_and_trip_mode;
create table boardings_by_agency_and_trip_mode as
SELECT 
	t.mode as trip_mode, 
	ta.agency as agency,
	pml.value_link_type as board_mode, 
	4*count(*) as board_count
FROM 
	"Path_Multimodal_links" pml,
	path_multimodal pm,
	trip t, 
	a.transit_trips tt, 
	a.transit_patterns tp, 
	a.transit_routes tr,
	a.transit_agencies ta
where 
	pml.value_est_Bus_Wait_Time + pml.value_est_rail_Wait_Time + pml.value_est_comm_rail_Wait_Time> 0 and
	pml.object_id = pm.id and
	pm.id = t.path_multimodal and
	pml.value_transit_vehicle_trip = tt.trip_id and 
	tp.pattern_id = tt.pattern_id and 
	tr.route_id = tp.route_id AND
	tr.agency_id = ta.agency_id
group by 
	ta.agency, 
	pml."value_link_type",	
	t.mode
;

drop table if exists boardings_by_agency_mode_time;
create table boardings_by_agency_mode_time as
SELECT 
	ta.agency as agency, 
	tv.mode as mode, 
	cast(cast(cast(tvl.value_act_departure_time as real)/1800 as int) as real)/2 as HH,
	4*sum(tvl.value_boardings) as boardings, 
	4*sum(tvl.value_alightings) as alightings
FROM 
	"Transit_Vehicle_links" tvl, 
	transit_vehicle tv, 
	a.transit_trips tt, 
	a.transit_patterns tp, 
	a.transit_routes tr,
	a.transit_agencies ta
where 
	tvl.value_transit_vehicle_trip = tv.transit_vehicle_trip and 
	tvl.value_transit_vehicle_trip = tt.trip_id and 
	tp.pattern_id = tt.pattern_id and 
	tr.route_id = tp.route_id AND
	tr.agency_id = ta.agency_id
group by 
	agency,
	mode,
	HH
order by
	agency,
	mode desc,
	HH
;

drop table if exists boardings_by_agency_mode_area_type;
create table boardings_by_agency_mode_area_type as
SELECT 
	ta.agency as agency, 
	tv.mode as mode,
	z.area_type as area_type,	
	4*sum(tvl.value_boardings) as boardings, 
	4*sum(tvl.value_alightings) as alightings
FROM 
	"Transit_Vehicle_links" tvl, 
	transit_vehicle tv, 
	a.transit_trips tt, 
	a.transit_patterns tp, 
	a.transit_routes tr,
	a.transit_agencies ta,
	a.transit_stops ts,
	a.transit_links tl,
	a.zone z
where 
	tvl.value_transit_vehicle_trip = tv.transit_vehicle_trip and 
	tvl.value_transit_vehicle_trip = tt.trip_id and 
	tp.pattern_id = tt.pattern_id and 
	tr.route_id = tp.route_id and 
	ta.agency_id = tr.agency_id AND
	tl.transit_link = tvl.value_link and
	ts.stop_id = tl.from_node and
	ts.zone = z.zone
group by 
	ta.agency,
	tv.mode,
	z.area_type
order by
	ta.agency,
	tv.mode desc,
	z.area_type
;

drop table if exists transit_occupancy;
create table transit_occupancy as
SELECT 
	ta.agency as agency,
	tv.mode as mode,
	4*0.000621371*sum((value_seated_load + value_standing_load) * value_length)/1000000 as PMT, 
	0.000621371*sum(value_length)/1000000 as VMT
FROM 
	"Transit_Vehicle_links" as tvl, 
	transit_vehicle tv,
	a.transit_trips as tt, 
	a.transit_patterns as tp, 
	a.transit_routes as tr,
	a.transit_agencies ta
where 
	tvl.value_transit_vehicle_trip = tv.transit_vehicle_trip and 
	tvl.value_transit_vehicle_trip = tt.trip_id and 
	tp.pattern_id = tt.pattern_id and 
	tr.route_id = tp.route_id AND
	ta.agency_id = tr.agency_id
group by 
	agency,
	mode
order by
	agency,
	mode desc 
;

drop table if exists act_wait_count;
create table act_wait_count as
select 4*sum(act_wait_count) as act_wait_count from path_multimodal;

drop table if exists avg_wait_and_total_time;
create table avg_wait_and_total_time as
select avg(act_bus_wait_time + act_rail_wait_time + act_comm_rail_wait_time)/60 as avg_wait_time, avg(act_bus_ivtt + act_rail_ivtt + act_comm_rail_ivtt)/60 as avg_ivtt, avg(act_bike_time+act_walk_time+act_car_time)/60 as avg_ovtt, avg(act_duration)/60 as avg_duration, avg(act_wait_count)-1 as avg_transfer_count from path_multimodal where act_bus_wait_time + act_rail_wait_time + act_comm_rail_wait_time > 0;

drop table if exists cta_IVTT_Wait_path_id_trip_id;
create table cta_IVTT_Wait_path_id_trip_id as
SELECT 
	"object_id" as path_id, 
	"value_transit_vehicle_trip" as vehicle_trip, 
	sum("value_est_Bus_Wait_Time" + "value_est_Rail_Wait_Time") as trip_wait_time, 
	sum("value_est_bus_IVTT" + "value_est_rail_IVTT") as trip_ivtt
FROM 
	"Path_Multimodal_links"
where 
	"value_transit_vehicle_trip" like '5%'
	and (value_est_Bus_Wait_Time >= 0.0 or value_est_Rail_Wait_Time >= 0.0)
	and (value_est_bus_IVTT >= 0.0 or value_est_rail_IVTT >= 0.0)
group by 
	"object_id", 
	"value_transit_vehicle_trip"
;

drop table if exists cta_IVTT_Wait_path_id_trips_aggregated;
create table cta_IVTT_Wait_path_id_trips_aggregated as
SELECT 
	c.path_id as path_id,
	sum(c.trip_wait_time) as cta_wait_time,
	sum(c.trip_ivtt) as cta_ivtt,
	count(c.path_id) as cta_boarding_count,
	(p.est_bus_wait_time + p.est_rail_wait_time) as wait_time,
	(p.est_bus_ivtt + p.est_rail_ivtt) as ivtt,
	p.est_bike_time + p.est_walk_time + p.est_car_time as ovtt,
	p.est_duration as duration,
	p.est_wait_count as boarding_count,
	p.number_of_switches as number_of_switches,
	(p.est_bus_wait_time + p.est_rail_wait_time) - sum(c.trip_wait_time) as wait_diff,
	(p.est_bus_ivtt + p.est_rail_ivtt) - sum(c.trip_ivtt) as ivtt_diff,
	p.est_wait_count- count(c.path_id) as board_diff
FROM 
	"CTA_IVTT_Wait_path_id_trip_id" as c,
	"path_multimodal" as p
where
	p.id = c.path_id
group by 
	c.path_id
;

drop table if exists avg_wait_and_total_time_cta;
create table avg_wait_and_total_time_cta as
select 
	avg(cta_wait_time)/60 as avg_wait_time, 
	avg(cta_ivtt)/60 as avg_ivtt, 
	avg(ovtt)/60 as avg_ovtt, 
	avg(duration)/60 as avg_duration, 
	avg(cta_boarding_count)-1 as avg_transfer_count, 
	4*sum(cta_boarding_count) as boarding_count,
	4*count(*) as trip_count,
	4*sum(number_of_switches) as reroute_count
from 
	cta_IVTT_Wait_path_id_trips_aggregated 
;

drop table if exists transit_trips_wo_boarding_count;
create table transit_trips_wo_boarding_count as
select 4*count(*) as transit_trips_wo_boarding_count from path_multimodal
where act_bus_ivtt + act_rail_ivtt + act_comm_rail_ivtt = 0 and mode <> 7 and mode <> 8;

drop table if exists transit_trips_with_boarding_count;
create table transit_trips_with_boarding_count as
select 4*count(*) as transit_trips_with_boarding_count from path_multimodal
where act_bus_ivtt + act_rail_ivtt + act_comm_rail_ivtt > 0 and mode <> 7 and mode <> 8;

drop table if exists gap_calculations;
create table gap_calculations as
SELECT
sum(case when has_artificial_trip = 0 then abs(end-start-routed_travel_time)
when has_artificial_trip = 1 then 0 
when has_artificial_trip = 2 then 2*routed_travel_time
when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) 
when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end)/sum(routed_travel_time) as relative_gap_abs,

sum(case when has_artificial_trip = 0 then max(end-start-routed_travel_time,0)
when has_artificial_trip = 1 then 0
when has_artificial_trip = 2 then 2*routed_travel_time
when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) 
when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end)/sum(routed_travel_time) as relative_gap_min0,

(sum(case when has_artificial_trip = 0 then end-start
when has_artificial_trip = 1 then routed_travel_time 
when has_artificial_trip = 2 then 3*routed_travel_time
when has_artificial_trip = 3 then max(end-start, routed_travel_time) 
when has_artificial_trip = 4 then max(end-start, routed_travel_time) end) - sum(routed_travel_time))/sum(routed_travel_time) as relative_gap,

sum(case when has_artificial_trip = 0 then end-start
when has_artificial_trip = 1 then routed_travel_time 
when has_artificial_trip = 2 then 3*routed_travel_time
when has_artificial_trip = 3 then max(end-start, routed_travel_time) 
when has_artificial_trip = 4 then max(end-start, routed_travel_time) end) as total_experienced_ttime,
sum(routed_travel_time) as total_routed_ttime,

sum(case when has_artificial_trip = 0 then abs(end-start-routed_travel_time)
when has_artificial_trip = 1 then 0 
when has_artificial_trip = 2 then 2*routed_travel_time
when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) 
when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end) as total_gap_abs,

sum(case when has_artificial_trip = 0 then max(end-start-routed_travel_time,0)
when has_artificial_trip = 1 then 0
when has_artificial_trip = 2 then 2*routed_travel_time
when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) 
when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end) as total_gap_min0,

count(*) as number_of_trips,
sum(case when path is not null then 1 end) as trips_with_path,
sum(case when path is null then 1 end) as trips_without_path,
sum(case when has_artificial_trip = 0 then 1 end) as all_good,
sum(case when has_artificial_trip = 1 then 1 end) as not_routed,
sum(case when has_artificial_trip = 2 then 1 end) as congestion_removal,
sum(case when has_artificial_trip = 3 then 1 end) as simulation_end,
sum(case when has_artificial_trip = 4 then 1 end) as stuck_in_entry_queue,

cast(sum(case when path is not null then 1 end)as real)/count(*) as perc_trips_with_path,
cast(sum(case when path is null then 1 end)as real)/count(*) as perc_trips_without_path,
cast(sum(case when has_artificial_trip = 0 then 1 end)as real)/count(*) as perc_all_good,
cast(sum(case when has_artificial_trip = 1 then 1 end)as real)/count(*) as perc_not_routed,
cast(sum(case when has_artificial_trip = 2 then 1 end)as real)/count(*) as perc_congestion_removal,
cast(sum(case when has_artificial_trip = 3 then 1 end)as real)/count(*) as perc_simulation_end,
cast(sum(case when has_artificial_trip = 4 then 1 end)as real)/count(*) as perc_stuck_in_entry_queue,

sum(case when has_artificial_trip = 0 then abs(end-start-routed_travel_time)
when has_artificial_trip = 1 then 0 
when has_artificial_trip = 2 then 2*routed_travel_time
when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) 
when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end)/count(*) as gap_per_trip

FROM "Trip"
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip <> 1 and end > start and routed_travel_time > 0;

drop table if exists gap_calculations_binned;
create table gap_calculations_binned as
SELECT
20*(case when has_artificial_trip = 0 then cast((end-start)/1200 as int)
when has_artificial_trip = 1 then cast((routed_travel_time)/1200 as int)
when has_artificial_trip = 2 then cast((3*routed_travel_time)/1200 as int)
when has_artificial_trip = 3 then cast((max(end-start, routed_travel_time))/1200 as int) 
when has_artificial_trip = 4 then cast((max(end-start, routed_travel_time))/1200 as int) end) as total_experienced_ttime_bin,
sum(case when has_artificial_trip = 0 then end-start
when has_artificial_trip = 1 then routed_travel_time 
when has_artificial_trip = 2 then 3*routed_travel_time
when has_artificial_trip = 3 then max(end-start, routed_travel_time) 
when has_artificial_trip = 4 then max(end-start, routed_travel_time) end) as total_experienced_ttime,
sum(routed_travel_time) as total_routed_ttime,
sum(case when has_artificial_trip = 0 then abs(end-start-routed_travel_time)
when has_artificial_trip = 1 then 0 
when has_artificial_trip = 2 then 2*routed_travel_time
when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) 
when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end) as total_gap_abs,
sum(case when has_artificial_trip = 0 then max(end-start-routed_travel_time,0)
when has_artificial_trip = 1 then 0
when has_artificial_trip = 2 then 2*routed_travel_time
when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) 
when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end) as total_gap_min0,
count(*) as number_of_trips,
sum(case when path is not null then 1 end) as trips_with_path,
sum(case when path is null then 1 end) as trips_without_path
FROM "Trip"
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip <> 1 and end > start and routed_travel_time > 0
group by total_experienced_ttime_bin;

drop table if exists there_is_path;
create table there_is_path as
select 
path is not null as there_is_path,
sum(abs(end-start-routed_travel_time))/sum(end-start) as relative_gap_abs,
sum(max(end-start-routed_travel_time,0))/sum(end-start) as relative_gap_min0,
count(*) as "number_of_trips" from trip
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20)  and has_artificial_trip = 0
group by there_is_path;

drop table if exists gap_bins;
create table gap_bins as
select cast(experienced_gap/0.1 as int)*0.1 as gap_bin, path is not null as there_is_path, count(*) from trip
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip = 0
group by gap_bin, there_is_path
order by gap_bin, there_is_path desc;

drop table if exists greater_routed_time;
create table greater_routed_time as
select routed_travel_time > (end-start) as greater_routed_time, count(*)
from trip
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip = 0
group by greater_routed_time;

drop table if exists mode_count;
create table mode_count as
select case mode
	when 0 then 'SOV'
	when 2 then 'HOV'
	when 4 then 'BUS'
	when 5 then 'RAIL'
	when 6 then 'NONMOTORIZED'
	when 7 then 'BICYCLE'
	when 8 then 'WALK'
	when 9 then 'TAXI'
	when 10 then 'SCHOOLBUS'
	when 11 then 'PARK_AND_RIDE'
	when 12 then 'KISS_AND_RIDE'
	when 13 then 'PARK_AND_RAIL'
	when 14 then 'KISS_AND_RAIL'
	when 15 then 'TNC_AND_RIDE'
	when 16 then 'TNC_AND_RAIL'
	when 25 then 'RIDE_AND_UNPARK'
	when 26 then 'RIDE_AND_REKISS'
	when 27 then 'RAIL_AND_UNPARK'
	when 28 then 'RAIL_AND_REKISS'
end as MODE_NAME, has_artificial_trip, 4*count(*) as mode_count from trip
group by MODE_NAME, has_artificial_trip
order by MODE_NAME, has_artificial_trip;

drop table if exists gap_breakdown;
create table gap_breakdown as
SELECT
sum(case when has_artificial_trip = 0 then end-start end) as total_experienced_ttime_0,
sum(case when has_artificial_trip = 1 then routed_travel_time end) as total_experienced_ttime_1,
sum(case when has_artificial_trip = 2 then 3*routed_travel_time end) as total_experienced_ttime_2,
sum(case when has_artificial_trip = 3 then max(end-start, routed_travel_time) end) as total_experienced_ttime_3,
sum(case when has_artificial_trip = 4 then max(end-start, routed_travel_time) end) as total_experienced_ttime_4,
sum(case when has_artificial_trip = 0 then routed_travel_time end) as total_routed_ttime_0,
sum(case when has_artificial_trip = 1 then routed_travel_time end) as total_routed_ttime_1,
sum(case when has_artificial_trip = 2 then routed_travel_time end) as total_routed_ttime_2,
sum(case when has_artificial_trip = 3 then routed_travel_time end) as total_routed_ttime_3,
sum(case when has_artificial_trip = 4 then routed_travel_time end) as total_routed_ttime_4,
sum(case when has_artificial_trip = 0 then abs(end-start-routed_travel_time) end) as total_gap_abs_0,
sum(case when has_artificial_trip = 1 then 0 end) as total_gap_abs_1,
sum(case when has_artificial_trip = 2 then 2*routed_travel_time end) as total_gap_abs_2,
sum(case when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) end) as total_gap_abs_3,
sum(case when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end) as total_gap_abs_4
FROM "Trip"
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip <> 1 and end > start and routed_travel_time > 0;

drop table if exists gap_breakdown_binned;
create table gap_breakdown_binned as
SELECT
20*(case when has_artificial_trip = 0 then cast((end-start)/1200 as int)
when has_artificial_trip = 1 then cast((routed_travel_time)/1200 as int)
when has_artificial_trip = 2 then cast((3*routed_travel_time)/1200 as int)
when has_artificial_trip = 3 then cast((max(end-start, routed_travel_time))/1200 as int) 
when has_artificial_trip = 4 then cast((max(end-start, routed_travel_time))/1200 as int) end) as total_experienced_ttime_bin,
sum(case when has_artificial_trip = 0 then end-start end) as total_experienced_ttime_0,
sum(case when has_artificial_trip = 1 then routed_travel_time end) as total_experienced_ttime_1,
sum(case when has_artificial_trip = 2 then 3*routed_travel_time end) as total_experienced_ttime_2,
sum(case when has_artificial_trip = 3 then max(end-start, routed_travel_time) end) as total_experienced_ttime_3,
sum(case when has_artificial_trip = 4 then max(end-start, routed_travel_time) end) as total_experienced_ttime_4,
sum(case when has_artificial_trip = 0 then routed_travel_time end) as total_routed_ttime_0,
sum(case when has_artificial_trip = 1 then routed_travel_time end) as total_routed_ttime_1,
sum(case when has_artificial_trip = 2 then routed_travel_time end) as total_routed_ttime_2,
sum(case when has_artificial_trip = 3 then routed_travel_time end) as total_routed_ttime_3,
sum(case when has_artificial_trip = 4 then routed_travel_time end) as total_routed_ttime_4,
sum(case when has_artificial_trip = 0 then abs(end-start-routed_travel_time) end) as total_gap_abs_0,
sum(case when has_artificial_trip = 1 then 0 end) as total_gap_abs_1,
sum(case when has_artificial_trip = 2 then 2*routed_travel_time end) as total_gap_abs_2,
sum(case when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) end) as total_gap_abs_3,
sum(case when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end) as total_gap_abs_4
FROM "Trip"
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip <> 1 and end > start and routed_travel_time > 0
group by total_experienced_ttime_bin;


drop table if exists transit_transfers_with_gen_cost;
create table transit_transfers_with_gen_cost as
SELECT
	cast((Est_Gen_Cost/3600) + 1 as int)*3600 as gen_cost_bin, 
	Est_Wait_Count, 
	Est_Transfer_Pen, 
	4*count(*) as trip_count
FROM
	"Path_Multimodal"
Where mode <> 8
GROUP BY
	gen_cost_bin,
	"Est_Wait_Count", 
	"Est_Transfer_Pen"
;
	
drop table if exists transit_transfers;
create table transit_transfers as
SELECT	
	Est_Wait_Count, 
	Est_Transfer_Pen, 
	4*count(*) as trip_count
FROM
	"Path_Multimodal"
Where mode <> 8
GROUP BY
	"Est_Wait_Count", 
	"Est_Transfer_Pen"
;

drop table if exists TNC_Agg_Output;
CREATE TABLE TNC_Agg_Output AS SELECT tt.*, pick_wait.*, sav_req.*,
(cast (avo_t.tot_pax as real)/ cast (avo_t.tot_trips as real)) as avo_trips,
avo_d.*,
(cast (avo_t_revenue.tot_pax as real)/ cast (avo_t_revenue.tot_trips as real)) as avo_trips_revenue,
avo_d_revenue.*,
sav4.*,
cast (sav4.avg_idle_time as real)/24*100 as perc_idle,
sav5.*,
sim_p.*, p.*, sov.*, hov.*, transit.*, sav1.*, sav2.*, sav3.*, sav_unmet.unmet_dist/avo_d.avo_dist as estimated_artificial_sav_vmt,
100*(cast (sav_req.demand as real)/(cast (sav_req.demand as real) + cast (sov_req.demand as real) + cast (hov_req.demand as real) + cast (non_motor_req.demand as real) + cast (transit_req.demand as real) + cast (other_req.demand as real))) as sav_mode_share,
100*(cast (sov_req.demand as real)/(cast (sav_req.demand as real) + cast (sov_req.demand as real) + cast (hov_req.demand as real) + cast (non_motor_req.demand as real) + cast (transit_req.demand as real) + cast (other_req.demand as real))) as sov_mode_share,
100*(cast (hov_req.demand as real)/(cast (sav_req.demand as real) + cast (sov_req.demand as real) + cast (hov_req.demand as real) + cast (non_motor_req.demand as real) + cast (transit_req.demand as real) + cast (other_req.demand as real)))  as hov_mode_share,
100*(cast (non_motor_req.demand as real)/(cast (sav_req.demand as real) + cast (sov_req.demand as real) + cast (hov_req.demand as real) + cast (non_motor_req.demand as real) + cast (transit_req.demand as real) + cast (other_req.demand as real)))  as non_motor_mode_share,
100*(cast (transit_req.demand as real)/(cast (sav_req.demand as real) + cast (sov_req.demand as real) + cast (hov_req.demand as real) + cast (non_motor_req.demand as real) + cast (transit_req.demand as real) + cast (other_req.demand as real)))  as transit_mode_share,
100*(cast (other_req.demand as real)/(cast (sav_req.demand as real) + cast (sov_req.demand as real) + cast (hov_req.demand as real) + cast (non_motor_req.demand as real) + cast (transit_req.demand as real) + cast (other_req.demand as real)))  as other_mode_share
FROM
(SELECT avg(end-start)/60 as TT FROM Trip WHERE mode = 9 AND has_artificial_trip = 0) as tt,
(SELECT avg(end-start)/60 as pickup,  avg(start - request_time)/60 as wait FROM TNC_Trip WHERE final_status = -1) as pick_wait,
(SELECT count(*) as demand, 100*(1- (cast (sum(has_artificial_trip) as real)) / (cast (count(*) as real))) as perc_met
FROM Trip t
WHERE mode = 9) as sav_req,
(SELECT count(*) as demand FROM Trip t
WHERE mode = 0) as sov_req,
(SELECT count(*) as demand FROM Trip t
WHERE mode = 2) as hov_req,
(SELECT count(*) as demand FROM Trip t
WHERE mode = 7 or mode = 8) as non_motor_req,
(SELECT count(*) as demand FROM Trip t
WHERE mode = 3 or mode = 4 or mode = 5 or mode = 11 or mode = 12 or mode = 13 or mode = 14 or mode = 15 or mode = 16) as transit_req,
(SELECT count(*) as demand FROM Trip t
WHERE mode = 10 or mode > 16) as other_req,
(SELECT sum(passengers) as tot_pax, count(*) as tot_trips FROM TNC_Trip WHERE travel_distance != 0) as avo_t,
(SELECT sum(passengers*travel_distance)/sum(travel_distance) as avo_dist FROM TNC_Trip) as avo_d,
(SELECT sum(passengers) as tot_pax, count(*) as tot_trips FROM TNC_Trip WHERE travel_distance != 0 and passengers != 0) as avo_t_revenue,
(SELECT sum(passengers*travel_distance)/sum(travel_distance) as avo_dist_revenue FROM TNC_Trip WHERE passengers != 0) as avo_d_revenue,
(SELECT sum(travel_distance)/1600 as sim_pmt FROM Trip WHERE has_artificial_trip = 0) as sim_p,
(SELECT sum(travel_distance)/1600 as tot_pmt FROM Trip) as p,
(SELECT sum(travel_distance)/1600 as sov_vmt FROM Trip WHERE mode = 0) as sov,
(SELECT sum(travel_distance)/1600 as hov_vmt FROM Trip WHERE mode = 2) as hov,
(SELECT sum(value_length)/1600 as transit_vmt FROM Transit_Vehicle_links) as transit,
(SELECT sum(travel_distance)/1600 as empty_sav_vmt FROM TNC_Trip WHEre passengers = 0) as sav1,
(SELECT sum(travel_distance)/1600 as repos_vmt FROM TNC_Trip WHEre final_status = -3) as sav2,
(SELECT sum(travel_distance)/1600 as sav_vmt FROM TNC_Trip) as sav3,
(SELECT avg(idle) as avg_idle_time FROM (SELECT 24 - sum(end-start)/3600 as idle From TNC_Trip Group by vehicle)) as sav4,
(SELECT avg(trips_completed) as avg_sav_trips FROM (SELECT count(*) as trips_completed From TNC_Trip where final_status = -2 group by vehicle)) as sav5,
(SELECT sum(travel_distance)/1600 as unmet_dist FROM Trip WHERE mode == 9 and has_artificial_trip != 0) as sav_unmet;

drop table if exists TNC_AVO_Revenue_by_TOD;
CREATE TABLE TNC_AVO_Revenue_by_TOD AS SELECT tod,
cast(tot_pax as real)/cast(tot_trips as real) as avo_t_revenue, tot_trips as demand FROM (SELECT cast(start/3600 as intc) as tod,
sum(passengers) as tot_pax, count(*) as tot_trips FROM TNC_Trip
WHERE travel_distance != 0 and passengers != 0 GROUP BY tod);

drop table if exists TNC_AVO_Dist_by_TOD;
CREATE TABLE TNC_AVO_Dist_by_TOD AS SELECT cast(start/3600 as int) as tod, sum(passengers * travel_distance)/sum(travel_distance) as avo_d
FROM TNC_Trip
GROUP BY tod;

drop table if exists TNC_Delay_Histogram;
CREATE TABLE TNC_Delay_Histogram AS SELECT count(*), cast(((end-start)/60)/5 as int) as tt FROM Trip
WHERE mode = 9 AND has_artificial_trip = 0 GROUP By tt;

drop table if exists TNC_Demand_by_TOD;
CREATE TABLE TNC_Demand_by_TOD AS SELECT cast(start/3600 as int) as tod, count(*) as demand
FROM Trip WHERE mode = 9 and has_artificial_trip = 0
GROUP BY tod;

drop table if exists TNC_Response_By_TOD;
CREATE TABLE TNC_Response_By_TOD AS SELECT cast(start/3600 as int) as hour, avg(end-request_time)/60 as response_time FROM TNC_Trip WHERE final_status = -1 GROUP BY hour;

drop table if exists employment_validation;
CREATE TABLE employment_validation as 
select l.zone, count(*)*4, z.employment_total
from person p, a.location l, a.zone z
where p.work_location_id = l.location  and l.zone = z.zone
group by l.zone;

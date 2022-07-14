drop table if exists gap_calculations;
create table gap_calculations as
SELECT
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
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip <> 1 and end > start and routed_travel_time > 0;

drop table if exists artificial_count;
create table artificial_count as
SELECT
count(*) as number_of_trips,
sum(case when has_artificial_trip = 0 then 1 end) as all_good,
sum(case when has_artificial_trip = 1 then 1 end) as not_routed,
sum(case when has_artificial_trip = 2 then 1 end) as congestion_removal,
sum(case when has_artificial_trip = 3 then 1 end) as simulation_end,
sum(case when has_artificial_trip = 4 then 1 end) as stuck_in_entry_queue
FROM "Trip"
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip <> 1 and end > start and routed_travel_time > 0;

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
	when 0 then "SOV"
	when 1 then "AUTO"
	when 2 then "HOV"
	when 3 then "TRUCK"
	when 4 then "BUS"
	when 5 then "RAIL"
	when 6 then "NONMOTORIZED"
	when 7 then "BICYCLE"
	when 8 then "WALK"
	when 9 then "TAXI"
	when 10 then "SCHOOLBUS"
	when 11 then "PARK_AND_RIDE"
	when 12 then "KISS_AND_RIDE"
	when 13 then "PARK_AND_RAIL"
	when 14 then "KISS_AND_RAIL"
	when 15 then "TNC_AND_RIDE"
	when 16 then "TNC_AND_RAIL"
	when 17 then "MD_TRUCK"	
	when 18 then "HD_TRUCK"
	when 19 then "BPLATE"
	when 20 then "LD_TRUCK"
	when 21 then "RAIL_NEST"
	when 22 then "BUS40"
	when 23 then "BUS60"
	when 25 then "TELEPORTED"
end as 'MODE', has_artificial_trip, count(*) from trip
group by mode, has_artificial_trip;

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

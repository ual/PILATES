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
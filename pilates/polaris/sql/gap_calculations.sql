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
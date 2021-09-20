pragma foreign_keys = off;

delete from activity;
delete from path;
delete from path_links;
delete from path_multimodal;
delete from path_multimodal_links;

drop table if exists Person_Gaps;
create table Person_Gaps (  
  "person" INTEGER NULL,
  "avg_gap" REAL NULL DEFAULT 0,  
  PRIMARY KEY("person"),
  CONSTRAINT "person_fk"
    FOREIGN KEY ("person")
    REFERENCES "Person" ("person")
    DEFERRABLE INITIALLY DEFERRED);

insert into Person_Gaps
SELECT
person,
sum(case when has_artificial_trip = 0 then max(end-start-routed_travel_time,0)
when has_artificial_trip = 1 then 0
when has_artificial_trip = 2 then 2*routed_travel_time
when has_artificial_trip = 3 then max(end-start-routed_travel_time, 0) 
when has_artificial_trip = 4 then max(end-start-routed_travel_time, 0) end)/sum(routed_travel_time) as avg_gap
FROM "Trip"
where (mode = 0 or mode = 9 or mode = 17 or mode = 18 or mode = 19 or mode = 20) and has_artificial_trip <> 1 and end > start and routed_travel_time > 0 and person is not null
group by person;

delete from trip where mode <> 0 and mode <> 17 and mode <> 18 and mode <> 19 and mode <> 20;
delete from trip where mode = 0 and person is not null;

update trip set vehicle = NULL, person = NULL, path = NULL, path_multimodal = NULL;

pragma foreign_keys = on;

pragma foreign_keys = off;

delete from activity;
delete from path;
delete from path_links;
delete from path_multimodal;
delete from path_multimodal_links;

delete from trip where mode <> 0 and mode <> 17 and mode <> 18 and mode <> 19 and mode <> 20;
delete from trip where mode = 0 and person is not null;

update trip set vehicle = NULL, person = NULL, path = NULL, path_multimodal = NULL, experienced_gap = 1.0;

pragma foreign_keys = on;

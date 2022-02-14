

At run time, input data for atlas expected to be found in this dir.

Expected files:

(1) accessbility.RData is #of jobs accessible within 30min public transit. data source: 2015 transit access by UM. 
this file will be replaced by beam processed accessiblity metrics later.

(2) modeaccessibility.csv includes indicators of avaiability of bus and rail stations, derived from the FHWA project. contact Qianmiao for more details. this file is only used for years <=2014. 

In the time of running, atlas preprocessor.py will extract data from urbansim h5 datastore and write as csv files in subfolders (e.g., year2010/*), including households.csv, persons.csv, jobs.csv, residential.csv, and blocks.csv.

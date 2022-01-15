

at run time, input data for atlas expected to be found in this dir.
If running in container, this dir is expected to be mounted from the host containing the data.

Expected files:

(1) accessbility.RData is #of jobs accessible within 30min public transit. data source: 2015 transit access by UM. 
this file will be replaced by beam processed accessiblity metrics later.

(2) modeaccessibility.csv includes indicators of avaiability of bus and rail stations, derived from the FHWA project. contact Qianmiao for more details. this file is only used for years <=2014. 

(3)atlas_input folder contained extracted datatables from urbansim and other models by year, they are processed by preprocess.py (which is to be added)

(4) atlas_output folder contained atlas output, including vehicle level prediction and household level prediction. 


PS.  Currently there is a .gitignore in this dir so that input files does not get accidentally checked into git.

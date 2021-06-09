# PILATES (v2)
**P**latform for \
**I**ntegrated \
**L**anduse \
**A**nd \
**T**ransportation \
**E**xperiments and \
**S**imulation

PILATES is designed to facilitate the integration of various containerized microsimulation applications that together fully model the co-evolution of land use and transportation systems over time for the purpose of long-term regional forecasting.

The PILATES Python library is comprised primarily of the following:
1. **run.py** -- An executable python script responisble for orchestrating the execution of all containerized applications and data-transformation steps.
2. **settings.yaml** -- A configuration file for managing and defining high-level system parameters (e.g. geographic region, local data paths, simulation time horizon, etc.)
3. **application-specific dirs** -- The subdirectories of `pilates/` contain application-specific I/O directories for mounting to their respective container images, as well as application-specific Python modules responsible for transforming and archiving I/O data generated/used by other component applications.
4. **utils/** -- A subdirectory of `pilates/` containing various Python modules that might be relevant to any or all of the component applications (e.g. common geographic data transformations or http requests to the US Census API.)


## 1. Setting up your environment
1. Make sure docker is running on your machine and that you've either pre-downloaded the required container images (specified in `settings.yaml`) or that you've signed into a valid docker account with dockerhub access.
2. Change other relevant parameters in `settings.yaml` (probably only [L2-10](https://github.com/ual/PILATES/blob/v2/settings.yaml#L2-L10))
   - UrbanSim settings of note:
      - `region_to_region_id` must have an entry that corresponds to the name of the input HDF5 datastore (see below)
   - ActivtySim settings of note:
      - num_processors: adjust this number to take full advantage of multi-threaded data processing in Python. Number should be close to the total number of virtual CPUs available on your machine (threads X cores x processors per core or something like that).
      - chunk_size: adjust this number to take full advantage of the available RAM on your machine. Trying making this number bigger until the activitysim container segfaults or is killed due to a memory error.
4. Make sure your Python environment has `docker-py`, and `pyyaml` installed.

## 2. I/O
PILATES needs to have two files in the local application directories in order to run:
1. **pilates/urbansim/data/custom_mpo_XXXX_model_data.h5** - an UrbanSim-formatted HDF5 datastore where `XXXX` is an MPO ID that must correspond to one of the region IDs specified in the UrbanSim settings ([L25](https://github.com/ual/PILATES/blob/master/settings.yaml#L25)) 
2. **pilates/beam/beam_outputs/XXXX.csv.gz** - the input skims file, where `XXXX` is the name of the skims file specified in the settings ([L14](https://github.com/ual/PILATES/blob/master/settings.yaml#L14)). 

With those two files in those two places, PILATES should handle the rest. 

NOTE: currently all input data is overwritten in place throughout the course of a multi-year PILATES run. To avoid data loss please store a local copy of the input data outside of PILATES.

## 3. Executing the full workflow
```
usage: run.py [-v] [-p] [-h HOUSEHOLD_SAMPLE_SIZE]

optional arguments:
  -v, --verbose         print docker stdout to the terminal
  -p, --pull_latest     pull latest docker images before running
```

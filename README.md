<p align="center"><img src="logo_multi.png" width="700"></p>

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
2. Change other relevant parameters in `settings.yaml` (probably only [L7-31](https://github.com/ual/PILATES/blob/v2/settings.yaml#L7-L30))
   - UrbanSim settings of note:
      - `region_to_region_id` must have an entry that corresponds to the name of the input HDF5 datastore (see below)
   - ActivtySim settings of note:
      - num_processors: adjust this number to take full advantage of multi-threaded data processing in Python. Number should be close to the total number of virtual CPUs available on your machine (threads X cores x processors per core or something like that).
      - chunk_size: adjust this number to take full advantage of the available RAM on your machine. Trying making this number bigger until the activitysim container segfaults or is killed due to a memory error.
4. Make sure your Python environment has `docker-py`, and `pyyaml` installed.

## 2. I/O
PILATES only needs two local data files in order to run: 1) an archive of land use and population tables corresponding to base year data for the specified region; and 2) a table of base-year travel skims in the format of the specified travel model. Currently, these two files are organized as follows:
1. **pilates/urbansim/data/custom_mpo_\<xxxxxxxx\>_model_data.h5** - an UrbanSim-formatted HDF5 datastore where `<xxxxxxxx>` is an 8-digit region ID corresponding to one of the IDs in the settings [L40](https://github.com/ual/PILATES/blob/master/settings.yaml#L40).
2. **pilates/\<travel model\>/\<travel model data dir\>/\<skims filename\>** - the input skims file, where `<skims filename>` is the name of the skims file specified in settings [L30](https://github.com/ual/PILATES/blob/master/settings.yaml#L31). Currently `polaris` and `beam` are the only supported travel models/skim formats.

With those two files in those two places, PILATES should handle the rest. 

NOTE: currently all input data is overwritten in place throughout the course of a multi-year PILATES run. To avoid data loss please store a local copy of the input data outside of PILATES.

## 3. Executing the full workflow
```
usage: ipython [-v] [-p] [-h HOUSEHOLD_SAMPLE_SIZE] [-s] [-w] [-d DISABLE_MODEL] [-c CONFIG]

optional arguments:
  -v, --verbose         print docker stdout
  -p, --pull_latest     pull latest docker images before running
  -h HOUSEHOLD_SAMPLE_SIZE, --household_sample_size HOUSEHOLD_SAMPLE_SIZE
                        household sample size (only works if land use models are disables)
  -s, --static_skims    bypass traffic assignment altogether (i.e. use base year skims for every run)
  -w, --warm_start_skims
                        generate full activity plans for the base year only. useful for generating warm start skims.
  -d DISABLE_MODEL, --disable_model DISABLE_MODEL
                        "l" for land use, "a" for activity demand, "t" for traffic assignment. Can specify multiple (e.g. "at")
  -c CONFIG, --config CONFIG
                        Specify different config .yaml (other than "settings.yaml")
```

## Miscellany

### ActivitySim BEAM integration
In order to have BEAM to run correctly one needs to set the following settings:

1. **skims_fname**: `gemini/10.activitySimODSkims.UrbanSim.TAZ.Full.csv.gz` The full skim file that contains all Origin Destinations pairs with ActivitySim path types.
2. **beam_config**: `gemini/activitysim-base-from-60k-input.conf` Path to beam config. This path must be relative to `beam_local_input_folder` and `region`. The BEAM docker container is provided with this config as an input.
3. **beam_scenario_folder**: `gemini/activitysim-plans-base-2010-cut-60k` Folder with BEAM scenario where ActivitySim output goes. Files from this folder are a scenario input for BEAM.
4. **beam_local_input_folder**: `pilates/beam/production/` Path to BEAM input folder. This folder is going to be mapped to the BEAM container input folder.
5. **beam_local_output_folder**: `pilates/beam/beam_output/` The BEAM output is going to be saved here. In order to have a clean run this directory should be empty before start.

#### BEAM Config: saves ASIM skims, enables BEAM to reuse the previous BEAM output plans, linkstats.

BEAM config should be set in the way so that BEAM saves ActivitySim skims, linkstats and loads people plans and linkstats from the previous runs.

This is the BEAM config options that enables it.

```hocon
# most of the time we need a single iteration
beam.agentsim.firstIteration = 0
beam.agentsim.lastIteration = 0

beam.router.skim = {
  # This allows to write skims on each iteration
  writeSkimsInterval = 1
}

beam.exchange{
  output {
    # this enables saving activitySim Skims
    activitySimSkimsEnabled = true
    # geo level different than TAZ (in beam taz-centers format)
    geo.filePath = ${beam.inputDirectory}"/block_group-centers.csv.gz"
  }
}

# This loads linkStats from the last found BEAM runs
beam.warmStart.type = "linkStatsFromLastRun"

# For subsequential beam runs (some data will be laoded from the latest found run in this directory)
beam.input.lastBaseOutputDir = ${beam.outputs.baseOutputDirectory}
# This prefix is used to find the last run output directory within beam.input.lastBaseOutputDir direcotry
beam.input.simulationPrefix = ${beam.agentsim.simulationName}

# fraction of input plans to be merged into the latest output plans (taken from the beam.input.lastBaseOutputDir)
beam.agentsim.agents.plans.merge.fraction = 0.2
```

#### Executing the simulation
```shell
nohup python run.py -v
```
nohup keeps the script working in case the user session is closed. The output is saved to nohup.out file by default.

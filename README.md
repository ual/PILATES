# PILATES (v2)
**P**latform for \
**I**ntegrated \
**L**anduse \
**A**nd \
**T**ransportation \
**E**xperiments and \
**S**imulation

PILATES is designed to facilitate the automated coordination of intra-simulation integration between UrbanSim, ActivitySim, and a user-specified travel demand model.

This repository is comprised primarily of the following:
1. **run.py** -- an executable python script responisble for orchestrating the execution of all simulations and intermediate data-transformation scripts managing command-line arguments and environment variables
2. **settings.yaml** -- a config file for managing and defining high-level system parameters (e.g. modeling region, data paths, simulation years, etc.)
3. **container dirs** -- [WORK IN PROGRESS] The subdirectories of `pilates/` contain container-specific file structures (for mounting) and Python modules responsible for transforming, archiving, and handing off the input/output data for each of the three major system components.



## 1. Setting up your environment
1. Make sure docker is running on your machine and that you've either pre-downloaded the required container images (specified in `settings.yaml`) or that you've signed into a valid docker account with dockerhub access.
2. Change other relevant parameters in `settings.yaml` (probably only [L2-10](https://github.com/ual/PILATES/blob/v2/settings.yaml#L2-L10))
3. Make sure your Python environment has `docker-py`, `s3fs`, and `pyyaml` installed.

## 2. I/O

### S3
PILATES is designed to read/write on AWS S3 unless otherwise specified. This means you must run PILATES on a machine with read/write access to the s3 buckets specified in `settings.yaml`. These buckets have to contain the base year input data, organized according to the filepath prefixes as specified in [line 73](https://github.com/ual/PILATES/blob/v2/run.py#L73) of `run.py`.
   - NOTE: PILATES uses the `s3fs` library to interact with AWS S3. See the [docs](https://s3fs.readthedocs.io/en/latest/#credentials) for more details about how PILATES expects `s3fs` to automatically load your AWS credentials.

### local [WORK IN PROGRESS]
If the right input data is stored in `pilates/activitysim/data`, the ActivitySim container will know to use the local copies rather than downloading from S3 on-the-fly when the local directory is mounted by docker. The local directory needs to have two files in it in order to avoid calling S3:
1. **model_data.h5** - an UrbanSim-formatted HDF5 datastore
2. **skims.omx** - the input skims file. This file can be generated directly from BEAM output skims using the `pilates/activitysim/preprocessor.py` module.

After the ActivitySim completes, two output files will be generated in `pilates/activitysim/output/`, **asim_outputs.zip** and **model_data.h5**, for use by BEAM and UrbanSim, respectively.

## 3. Executing the full workflow
```
usage: run.py [-v] [-p] [-h HOUSEHOLD_SAMPLE_SIZE]

optional arguments:
  -v, --verbose         print docker stdout
  -p, --pull_latest     pull latest docker images before running
  -h HOUSEHOLD_SAMPLE_SIZE, --household_sample_size HOUSEHOLD_SAMPLE_SIZE
                        household sample size
```
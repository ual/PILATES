# PILATES (v2)
**P**latform for \
**I**ntegrated \
**L**anduse \
**A**nd \
**T**ransportation \
**E**xperiments and \
**S**imulation

PILATES is designed to facilitate the automated coordination of intra-simulation integration between UrbanSim, ActivitySynth, and a user-specified travel demand model.

This repository is comprised primarily of the following:
1. **pilates.py** -- a single executable bash script responisble for orchestrating the execution of all simulations and intermediate data-transformation scripts managing command-line arguments and environment variables
2. **settings.yaml** -- a config file for managing and defining high-level system parameters (e.g. modeling region, data paths, simulation years, etc.)



## Running PILATES

### Setting up your environment
1. Make sure docker is running on your machine and that you've either pre-downloaded the required container images (specified in `settings.yaml`) or that you've signed into a valid docker account with dockerhub access.
2. Make sure your Python environment has `docker-py`, `s3fs`, and `pyyaml` installed.
3. Make sure you are running PILATES on an ec2 instance with read/write access to the s3 buckets specified in `settings.yaml`. These buckets must contain the base year input data, organized using filepath prefixes as specified in [line 33](https://github.com/ual/PILATES/blob/v2/pilates.py#L33) of `pilates.py`.
   - NOTE: if your ec2 instance is associated with the same AWS organization as the s3 buckets this should work automatically. If not, you can simply sign into the right AWS account using the AWS CLI, and `s3fs` should be able to read the credentials from your session. If that still doesn't work, you can always pass the right AWS credentials to the s3fs client directly in the `pilates.py` script.

### Executing the full workflow
Simply run: `python pilates.py` with optional flags `-v` for verbose output and `-p` to force docker to pull the latest versions of all specified images.

# TO DO:

- [ ] delete old inputs before copying new ones so that if a process fails the next one won't use out-dated results.
- [x] convert asim output to .h5 only, handle beam/usim output conversion solely in pilates
- [ ] Fix stdout print so it doesn't look ugly
- [ ] Fix pyproj errors in usim image
